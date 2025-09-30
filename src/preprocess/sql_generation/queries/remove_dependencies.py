from src.preprocess.sql_generation.sql_common import write


def remove_all_unnecessary_dependencies(f, preprocess_api_version):
    write(f, f"""
    SELECT 'Starting DELETE DEPENDENCIES Block version: {preprocess_api_version}' AS info;
    """)
    if preprocess_api_version == 34:
        write(f, """
DELETE FROM programstageinstance_messageconversation;
DELETE FROM programinstancecomments;
DELETE FROM programinstanceaudit;
DELETE FROM datavalueaudit;
DELETE FROM trackedentitydatavalueaudit;
DELETE FROM trackedentityattributevalueaudit;
DELETE FROM programstageinstance_messageconversation;
DELETE FROM dataapprovalaudit;
DELETE FROM interpretation_comments;
DELETE FROM interpretationcomment;
DELETE FROM messageconversation_messages;
DELETE FROM messageconversation_usermessages;
DELETE FROM messageconversation;
    """)
    elif preprocess_api_version < 40:
        write(f, """
DELETE FROM programstageinstance_messageconversation;
DELETE FROM programinstancecomments;
DELETE FROM datavalueaudit;
DELETE FROM trackedentitydatavalueaudit;
DELETE FROM trackedentityattributevalueaudit;
DELETE FROM programstageinstance_messageconversation;
DELETE FROM dataapprovalaudit;
DELETE FROM interpretation_comments;
DELETE FROM interpretationcomment;
DELETE FROM interpretationusergroupaccesses;
DELETE FROM intepretation_likedby;
DELETE FROM messageconversation_messages;
DELETE FROM messageconversation_usermessages;
DELETE FROM messageconversation;
    """)
    else:
        write(f, """
DELETE FROM datavalueaudit;
DELETE FROM trackedentitydatavalueaudit;
DELETE FROM trackedentityattributevalueaudit;
DELETE FROM dataapprovalaudit;
DELETE FROM interpretation_comments;
DELETE FROM interpretationcomment;
DELETE FROM intepretation_likedby;
DELETE FROM messageconversation_messages;
DELETE FROM messageconversation_usermessages;
DELETE FROM messageconversation;
delete from event_notes;
delete from enrollment_notes;
delete from note;
delete from trackedentityaudit;
delete from programmessage_phonenumbers;
delete from programmessage_emailaddresses;
delete from programmessage_deliverychannels;
delete from programmessage;
delete from programownershiphistory;
delete from programtempownershipaudit;
    """)
    write(f, f"""
        SELECT 'Close DELETE DEPENDENCIES Block' AS info;
        """)
