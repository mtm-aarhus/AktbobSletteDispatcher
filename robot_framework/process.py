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

    cursor = pyodbc.connect(connection_string).cursor()

    dont_connect = False
    if dont_connect:
        Cases = []
        with open("cases_raw.txt", "r", encoding="utf-8") as f:
            for line in f:
                Cases.append(json.loads(line.strip()))
    else:
        Cases = GetDeskProIDs(cursor)

        for case in Cases:
            DeskProID = case["DeskproId"]

            url = f"{DeskproAPIURL}/api/v2/tickets/{DeskProID}"
            headers = {
                "Authorization": DeskProAPIKey,
                "Cookie": "dp_last_lang=da"
            }
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                status = e.response.status_code if e.response is not None else None
                orchestrator_connection.log_info(f'{DeskProID} ikke fundet / fejl: {e}')
                if status == 404:
                    orchestrator_connection.log_info('Sagen er ikke fundet - søger')
                    url_404 = f"{DeskproAPIURL}/api/v2/search?q={DeskProID}&types=ticket"
                    headers = {
                        "Authorization": DeskProAPIKey,
                        "Cookie": "dp_last_lang=da"
                    }
                    try:
                        response = requests.get(url_404, headers=headers, timeout=10).json()
                
                        ticket_group = next(
                           (g for g in response["data"]["grouped_results"] if g["type"] == "ticket"), None)

                        har_resultat = bool(ticket_group and ticket_group["results"])

                        if har_resultat:
                            case["Afslutningsdato"] = None
                        else:
                            case["Afslutningsdato"] = "2000-01-01T00:00:00+0000"

                    except:
                        case["Afslutningsdato"] = None
                continue

            data = response.json()
            fields = data.get("data", {}).get("fields", {})
            final_date = fields.get("180", {}).get("value", "")

            case["Afslutningsdato"] = final_date
            time.sleep(0.3)

        # gem alt ned, så du kan køre videre uden API-kald næste gang
        # with open("cases_raw.txt", "w", encoding="utf-8") as f:
        #     for case in Cases:
        #         f.write(json.dumps(case, ensure_ascii=False) + "\n")

    # filtrér på afslutningsdato
    filtered_by_id = {}

    for case in Cases:
        raw = case.get("Afslutningsdato")
        if not raw:
            continue

        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S%z")

        if dt < datetime.now(dt.tzinfo) - timedelta(days=61):
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
        if case["SharepointFolderName"]:
            queue_json = {
                "DeskproId": case["DeskproId"],
                "SharepointFolderName": case["SharepointFolderName"]
            }

            orchestrator_connection.create_queue_element("DeleteSharepointFolder", "NEW", json.dumps(queue_json))
            # print(f'Jeg sletter {case["DeskproId"]} m {case["SharepointFolderName"]}')
        if case["FilArkivCaseIds"]:
            for id in case["FilArkivCaseIds"]:
                # print(f'Jeg sletter {case["DeskproId"]} m {id}')
                queue_json = {
                    "DeskproId": case["DeskproId"],
                    "FilarkivCaseId": id
                }

                orchestrator_connection.create_queue_element("DeleteFilArkivCase", "NEW", json.dumps(queue_json))