from src import dhis2db


def preprocess(cfg, entries):
    db_con = dhis2db.init_db(cfg["database"], cfg["username"], cfg["password"], cfg["host"], cfg["port"])
    try:
        for entry in [x for x in entries]:
            execute(db_con, entry)
    finally:
        db_con.close()


def execute(db_con, entry):
    "Execute the action described in one entry of the postprocessing"
    get = lambda x: entry.get(x, [])

    # TODO get list of eventPrograms to exclude or randomize
    # TODO get list of datasets to exclude or randomize

    action = get("action")
    if action == "removeDataValues":
        removeDataValues(db_con, get("excludeDatasets"))
    elif action == "removeAuditDataValues":
        removeAuditDataValues(db_con, get("excludeDatasets"))
    elif action == "removeEvents":
        removeEvents(db_con, get("excludePrograms"))
    elif action == "removeAuditEvents":
        removeAuditEvents(db_con, get("excludePrograms"))


def removeDataValues(db_con, excludeDatasets):
    db_con.execute(
        "delete from datavalue where dataelementid not in (select dse.dataelementid from datasetelement dse inner join dataset ds on ds.datasetid=dse.datasetid where ds.uid in (" + excludeDatasets + "));")


def removeAuditDataValues(db_con, excludeDatasets):
    db_con.execute(
        "delete from datavalueaudit where dataelementid not in (select dse.dataelementid from datasetelement dse inner join dataset ds on ds.datasetid=dse.datasetid where ds.uid in (" + excludeDatasets + "));")


def removeEvents(db_con, excludePrograms):
    db_con.execute(
        "update programstageinstance set eventdatavalues='' where programinstanceid not in (select programinstanceid from programinstance pi inner join program p on p.programid=pi.programid where p.uid not in (" + excludePrograms + "));")
#update programstageinstance set eventdatavalues='' where programinstanceid not in (select programinstanceid from programinstance pi inner join program p on p.programid=pi.programid where p.uid not in  ('G9hvxFI8AYC'));

def removeAuditEvents(db_con, excludePrograms):
    db_con.execute(
        "delete from trackedentitydatavalueaudit where programstageinstanceid in (select programstageinstanceid from programstageinstance where programinstanceid not in (select programinstanceid from programinstance pi inner join program p on p.programid=pi.programid where p.uid not in (" + excludePrograms + ")));")
#delete from trackedentitydatavalueaudit where programstageinstanceid in (select programstageinstanceid from programstageinstance where programinstanceid not in (select programinstanceid from programinstance pi inner join program p on p.programid=pi.programid where p.uid not in ('G9hvxFI8AYC')));"
# update programstageinstance set eventdatavalues='' where programinstanceid not in (select programinstanceid from programinstance pi inner join program p on p.programid=pi.programid where p.uid not in  ('G9hvxFI8AYC'));
