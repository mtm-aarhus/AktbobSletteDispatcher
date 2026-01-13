from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import os
import requests
import json
import pytz
import pyodbc
import time
from datetime import datetime, timedelta


def GetDeskProIDs(cursor):
    cursor.execute("""
        SELECT Id, DeskproId, SharepointFolderName
        FROM dbo.Tickets
        WHERE SlettetSharepoint = 0
        OR SlettetFilArkiv = 0
    """)

    rows = cursor.fetchall()

    result = []
    for r in rows:
        d = {
            "Id": r.Id,
            "DeskproId": r.DeskproId,
            "SharepointFolderName": r.SharepointFolderName
        }
        result.append(d)

    return result


def process(orchestrator_connection: OrchestratorConnection) -> None:
    DeskproAPIURL = orchestrator_connection.get_constant('DeskproAPIURL').value
    DeskProAPI = orchestrator_connection.get_credential("DeskProAPI")
    DeskProAPIKey = DeskProAPI.password

    server = orchestrator_connection.get_constant('AktbobServer').value
    database = orchestrator_connection.get_constant('AktbobDatabase').value
    databasebruger = orchestrator_connection.get_credential('AktbobDatabaseBruger')

    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server=tcp:{server}.database.windows.net,1433;"
        f"Database={database};"
        f"Uid={databasebruger.username};"
        f"Pwd={databasebruger.password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    # Gem både connection og cursor (vi skal bruge connection til commit)
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    dont_connect = False
    if dont_connect:
        Cases = []
        with open("cases_raw.txt", "r", encoding="utf-8") as f:
            for line in f:
                Cases.append(json.loads(line.strip()))
    else:
        Cases = GetDeskProIDs(cursor)

        # Fælles headers til alle Deskpro-kald
        headers = {
            "Authorization": DeskProAPIKey,
            "Cookie": "dp_last_lang=da"
        }

        for case in Cases:
            DeskProID = case["DeskproId"]

            url = f"{DeskproAPIURL}/api/v2/tickets/{DeskProID}"

            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                status = e.response.status_code if e.response is not None else None
                orchestrator_connection.log_info(f'{DeskProID} ikke fundet / fejl: {e}')
                if status == 404:
                    orchestrator_connection.log_info('Sagen er ikke fundet - søger')
                    url_404 = f"{DeskproAPIURL}/api/v2/search?q={DeskProID}&types=ticket"

                    try:
                        search_response = requests.get(url_404, headers=headers, timeout=10).json()

                        ticket_group = next(
                            (g for g in search_response["data"]["grouped_results"] if g["type"] == "ticket"),
                            None
                        )

                        har_resultat = bool(ticket_group and ticket_group["results"])

                        if har_resultat:
                            case["Afslutningsdato"] = None
                        else:
                            # Behandl som "meget gammel" / udløbet
                            case["Afslutningsdato"] = "2000-01-01T00:00:00+0000"

                    except Exception as e2:
                        orchestrator_connection.log_info(f'Fejl ved 404-søgning for {DeskProID}: {e2}')
                        case["Afslutningsdato"] = None
                continue

            data = response.json()
            fields = data.get("data", {}).get("fields", {})
            final_date = fields.get("180", {}).get("value", "")

            workflow = fields.get("48", {})
            details = workflow.get("detail", {})

            is_faerdigbehandlet = any(
                d.get("title") == "Aktindsigt færdigbehandlet"
                for d in details.values()
            )

            if not is_faerdigbehandlet:
                orchestrator_connection.log_info(f'Sagen {DeskProID} er ikke markeret færdigbehandlet, og springes derfor over')
                continue

            case["Afslutningsdato"] = final_date
            time.sleep(0.3)

        # gem alt ned, så du kan køre videre uden API-kald næste gang
        # with open("cases_raw.txt", "w", encoding="utf-8") as f:
        #     for case in Cases:
        #         f.write(json.dumps(case, ensure_ascii=False) + "\n")

    # filtrér på afslutningsdato
    filtered_by_id = {}

    # Brug en fælles cutoff-tid
    now_utc = datetime.now(pytz.utc)
    cutoff = now_utc - timedelta(days=45)

    for case in Cases:
        raw = case.get("Afslutningsdato")
        if not raw:
            continue

        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S%z")

        if dt < cutoff:
            filtered_by_id[case["Id"]] = case  # overskriv evt. dubletter

    filtered = list(filtered_by_id.values())

    expired_ids = [case["Id"] for case in filtered]

    if expired_ids:  # vigtig: undgå IN ()
        placeholders = ",".join("?" for _ in expired_ids)

        query = f"""
            SELECT 
                t.Id AS TicketId,
                t.SharepointFolderName AS TicketSharepointFolderName,
                t.SlettetSharepoint,
                t.SlettetFilArkiv,
                c.FilArkivCaseId
            FROM dbo.Tickets t
            LEFT JOIN dbo.Cases c 
                ON c.TicketId = t.Id
            WHERE t.Id IN ({placeholders})
        """

        cursor.execute(query, expired_ids)
        rows = cursor.fetchall()
    else:
        rows = []

    lookup = {}

    for row in rows:
        tid = row.TicketId

        if tid not in lookup:
            lookup[tid] = {
                "SharepointFolderName": None,
                "FilArkivCaseIds": set()
            }

        # SharePoint: kun hvis IKKE slettet
        if row.SlettetSharepoint == 0:
            lookup[tid]["SharepointFolderName"] = row.TicketSharepointFolderName

        # FilArkiv: kun hvis IKKE slettet
        if row.SlettetFilArkiv == 0 and row.FilArkivCaseId is not None:
            lookup[tid]["FilArkivCaseIds"].add(row.FilArkivCaseId)

    for case in filtered:
        tid = case["Id"]
        meta = lookup.get(tid)

        if meta:
            case["SharepointFolderName"] = meta["SharepointFolderName"]
            case["FilArkivCaseIds"] = sorted(meta["FilArkivCaseIds"])
        else:
            case["SharepointFolderName"] = None
            case["FilArkivCaseIds"] = []

        # Opret køelementer til sletning i SharePoint / FilArkiv
        if case["SharepointFolderName"]:
            queue_json = {
                "DeskproId": case["DeskproId"],
                "SharepointFolderName": case["SharepointFolderName"]
            }

            orchestrator_connection.create_queue_element(
                "DeleteSharepointFolder",
                "NEW",
                json.dumps(queue_json)
            )

        if case["FilArkivCaseIds"]:
            for fid in case["FilArkivCaseIds"]:
                queue_json = {
                    "DeskproId": case["DeskproId"],
                    "FilarkivCaseId": fid
                }

                orchestrator_connection.create_queue_element(
                    "DeleteFilArkivCase",
                    "NEW",
                    json.dumps(queue_json)
                )

    # Markér udløbne sager UDEN nogen FilArkiv-case som slettet i databasen
    no_filarkiv_ids = [
        case["Id"]
        for case in filtered
        if not case["FilArkivCaseIds"]  # tom liste => ingen FilArkiv-sager
    ]

    if no_filarkiv_ids:
        placeholders = ",".join("?" for _ in no_filarkiv_ids)
        update_sql = f"""
            UPDATE dbo.Tickets
            SET SlettetFilArkiv = 1
            WHERE Id IN ({placeholders})
              AND SlettetFilArkiv = 0
        """
        cursor.execute(update_sql, no_filarkiv_ids)

    # Markér udløbne sager UDEN SharePoint-mappe som slettet i databasen
    no_sharepoint_ids = [
        case["Id"]
        for case in filtered
        if not case["SharepointFolderName"]  # None eller tom streng => ingen mappe
    ]

    if no_sharepoint_ids:
        placeholders = ",".join("?" for _ in no_sharepoint_ids)
        update_sql_sp = f"""
            UPDATE dbo.Tickets
            SET SlettetSharepoint = 1
            WHERE Id IN ({placeholders})
              AND SlettetSharepoint = 0
        """
        cursor.execute(update_sql_sp, no_sharepoint_ids)

    # Commit alle ændringer i databasen
    conn.commit()
