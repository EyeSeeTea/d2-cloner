-- Autogenerated preprocess sql file
delete from programstageinstance_messageconversation;
delete from programinstancecomments;
delete from programinstanceaudit;
delete from datavalueaudit;
delete from trackedentitydatavalueaudit;
delete from trackedentityattributevalueaudit;
delete from programstageinstance_messageconversation;
delete from dataapprovalaudit;
delete from interpretation_comments;
delete from interpretationcomment;
delete from messageconversation_messages;
delete from messageconversation_usermessages;
delete from messageconversation;
--NTD
--remove all tracker
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
delete from programstageinstancecomments where programstageinstanceid in ( select programstageinstanceid from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'))));
delete from trackedentityattributevalue where trackedentityinstanceid in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij')));
delete from trackedentityattributevalueaudit where trackedentityinstanceid in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij')));
delete from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij')));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
create view tei_to_remove as select trackedentityinstanceid "teiid" from programinstance where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
delete from programinstance where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);
delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);
delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);
drop view tei_to_remove ;
--remove tracker finish
--remove all events
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('vFcxda6dJyT', 'RS6DH7eJJMZ', 'i5JSf4ffFl2', 'gxXvKE74vfQ', 'AODL0NZqmGx', 'MECZlaC9Ef6', 'JOwDQ193x2T', 'ela4dpipbEm', 'fQTmlJy1rLT', 'TFTLPseQEgi', 'uYKG1KOUIy1', 'h1KVXwrpKKf', 'dBAcTWcDpbO', 'NCGFNCiONAq', 'a7ygv0UrrSK', 'AOGxHgdhbYD', 'k3pL2hPUqQi', 'Jd8gnEIt8uT', 'nk0PS9gG8dy', 'EwH68ZzyDRk', 'Mvj9zd9l5g2', 'NVUlJzIakuO', 'IPEvFBRqJjW', 'w9hSFsNr3Vh', 'XQl1AKLPgy0', 'pvYIJL19r6G', 'qdlYvVgUjU7', 'wlWD2hWVghi', 'z6skqIipb47', 't1Yjgl9MeOE', 'CVh71SDylDD', 'M5pXkRoE62G', 'K45hjkzs0WX'));
delete from programstageinstancecomments where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('vFcxda6dJyT', 'RS6DH7eJJMZ', 'i5JSf4ffFl2', 'gxXvKE74vfQ', 'AODL0NZqmGx', 'MECZlaC9Ef6', 'JOwDQ193x2T', 'ela4dpipbEm', 'fQTmlJy1rLT', 'TFTLPseQEgi', 'uYKG1KOUIy1', 'h1KVXwrpKKf', 'dBAcTWcDpbO', 'NCGFNCiONAq', 'a7ygv0UrrSK', 'AOGxHgdhbYD', 'k3pL2hPUqQi', 'Jd8gnEIt8uT', 'nk0PS9gG8dy', 'EwH68ZzyDRk', 'Mvj9zd9l5g2', 'NVUlJzIakuO', 'IPEvFBRqJjW', 'w9hSFsNr3Vh', 'XQl1AKLPgy0', 'pvYIJL19r6G', 'qdlYvVgUjU7', 'wlWD2hWVghi', 'z6skqIipb47', 't1Yjgl9MeOE', 'CVh71SDylDD', 'M5pXkRoE62G', 'K45hjkzs0WX'));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('vFcxda6dJyT', 'RS6DH7eJJMZ', 'i5JSf4ffFl2', 'gxXvKE74vfQ', 'AODL0NZqmGx', 'MECZlaC9Ef6', 'JOwDQ193x2T', 'ela4dpipbEm', 'fQTmlJy1rLT', 'TFTLPseQEgi', 'uYKG1KOUIy1', 'h1KVXwrpKKf', 'dBAcTWcDpbO', 'NCGFNCiONAq', 'a7ygv0UrrSK', 'AOGxHgdhbYD', 'k3pL2hPUqQi', 'Jd8gnEIt8uT', 'nk0PS9gG8dy', 'EwH68ZzyDRk', 'Mvj9zd9l5g2', 'NVUlJzIakuO', 'IPEvFBRqJjW', 'w9hSFsNr3Vh', 'XQl1AKLPgy0', 'pvYIJL19r6G', 'qdlYvVgUjU7', 'wlWD2hWVghi', 'z6skqIipb47', 't1Yjgl9MeOE', 'CVh71SDylDD', 'M5pXkRoE62G', 'K45hjkzs0WX'));
--remove all datasets
delete from datavalueaudit where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('NePI9W0zSH5', 'UFlGxXoBrLN', 'ZO0DQJePIEH', 'Psf9CW4BSHM', 'Uc3j0vpsfSB', 'LdQ3CckNpeQ', 'tnek2LjfuIm', 'zna8KfLMXn4', 'XBgvNrxpcDC', 'WHPEpoVDFFv', 'SAV16xEdCZW', 'AAYgHGENgbF', 'ZhvE3MziQkC', 'yOf6XrrS3uy', 'NKWbkXyfO5F', 'p0NhuIUoeST', 'oVxjBKA1Yzu', 'deKCGAGoEHz', 'S1UMweeoPsi', 'fdBM4sWSuPR', 'SHw2zOysJ1R', 's3iaozBY0dv', 'JP4bMwvJ6oU', 'U5ejGQdX4Ih')));
delete from datavalue where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('NePI9W0zSH5', 'UFlGxXoBrLN', 'ZO0DQJePIEH', 'Psf9CW4BSHM', 'Uc3j0vpsfSB', 'LdQ3CckNpeQ', 'tnek2LjfuIm', 'zna8KfLMXn4', 'XBgvNrxpcDC', 'WHPEpoVDFFv', 'SAV16xEdCZW', 'AAYgHGENgbF', 'ZhvE3MziQkC', 'yOf6XrrS3uy', 'NKWbkXyfO5F', 'p0NhuIUoeST', 'oVxjBKA1Yzu', 'deKCGAGoEHz', 'S1UMweeoPsi', 'fdBM4sWSuPR', 'SHw2zOysJ1R', 's3iaozBY0dv', 'JP4bMwvJ6oU', 'U5ejGQdX4Ih')));
--remove all tracker
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
delete from programstageinstancecomments where programstageinstanceid in ( select programstageinstanceid from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'))));
delete from trackedentityattributevalue where trackedentityinstanceid in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij')));
delete from trackedentityattributevalueaudit where trackedentityinstanceid in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij')));
delete from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij')));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
create view tei_to_remove as select trackedentityinstanceid "teiid" from programinstance where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
delete from programinstance where programid in (select programid from program where uid in ('uGCVVme3C4e', 'NHy39qEl1bc', 'KHDErSOFmy5', 'dIr4OtNcvMw', 'HnA4SUOaTij'));
delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);
delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);
delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);
drop view tei_to_remove ;
--remove tracker finish
--remove all events
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('vFcxda6dJyT', 'RS6DH7eJJMZ', 'i5JSf4ffFl2', 'gxXvKE74vfQ', 'AODL0NZqmGx', 'MECZlaC9Ef6', 'JOwDQ193x2T', 'ela4dpipbEm', 'fQTmlJy1rLT', 'TFTLPseQEgi', 'uYKG1KOUIy1', 'h1KVXwrpKKf', 'dBAcTWcDpbO', 'NCGFNCiONAq', 'a7ygv0UrrSK', 'AOGxHgdhbYD', 'k3pL2hPUqQi', 'Jd8gnEIt8uT', 'nk0PS9gG8dy', 'EwH68ZzyDRk', 'Mvj9zd9l5g2', 'NVUlJzIakuO', 'IPEvFBRqJjW', 'w9hSFsNr3Vh', 'XQl1AKLPgy0', 'pvYIJL19r6G', 'qdlYvVgUjU7', 'wlWD2hWVghi', 'z6skqIipb47', 't1Yjgl9MeOE', 'CVh71SDylDD', 'M5pXkRoE62G', 'K45hjkzs0WX'));
delete from programstageinstancecomments where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('vFcxda6dJyT', 'RS6DH7eJJMZ', 'i5JSf4ffFl2', 'gxXvKE74vfQ', 'AODL0NZqmGx', 'MECZlaC9Ef6', 'JOwDQ193x2T', 'ela4dpipbEm', 'fQTmlJy1rLT', 'TFTLPseQEgi', 'uYKG1KOUIy1', 'h1KVXwrpKKf', 'dBAcTWcDpbO', 'NCGFNCiONAq', 'a7ygv0UrrSK', 'AOGxHgdhbYD', 'k3pL2hPUqQi', 'Jd8gnEIt8uT', 'nk0PS9gG8dy', 'EwH68ZzyDRk', 'Mvj9zd9l5g2', 'NVUlJzIakuO', 'IPEvFBRqJjW', 'w9hSFsNr3Vh', 'XQl1AKLPgy0', 'pvYIJL19r6G', 'qdlYvVgUjU7', 'wlWD2hWVghi', 'z6skqIipb47', 't1Yjgl9MeOE', 'CVh71SDylDD', 'M5pXkRoE62G', 'K45hjkzs0WX'));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('vFcxda6dJyT', 'RS6DH7eJJMZ', 'i5JSf4ffFl2', 'gxXvKE74vfQ', 'AODL0NZqmGx', 'MECZlaC9Ef6', 'JOwDQ193x2T', 'ela4dpipbEm', 'fQTmlJy1rLT', 'TFTLPseQEgi', 'uYKG1KOUIy1', 'h1KVXwrpKKf', 'dBAcTWcDpbO', 'NCGFNCiONAq', 'a7ygv0UrrSK', 'AOGxHgdhbYD', 'k3pL2hPUqQi', 'Jd8gnEIt8uT', 'nk0PS9gG8dy', 'EwH68ZzyDRk', 'Mvj9zd9l5g2', 'NVUlJzIakuO', 'IPEvFBRqJjW', 'w9hSFsNr3Vh', 'XQl1AKLPgy0', 'pvYIJL19r6G', 'qdlYvVgUjU7', 'wlWD2hWVghi', 'z6skqIipb47', 't1Yjgl9MeOE', 'CVh71SDylDD', 'M5pXkRoE62G', 'K45hjkzs0WX'));
--remove all datasets
delete from datavalueaudit where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('NePI9W0zSH5', 'UFlGxXoBrLN', 'ZO0DQJePIEH', 'Psf9CW4BSHM', 'Uc3j0vpsfSB', 'LdQ3CckNpeQ', 'tnek2LjfuIm', 'zna8KfLMXn4', 'XBgvNrxpcDC', 'WHPEpoVDFFv', 'SAV16xEdCZW', 'AAYgHGENgbF', 'ZhvE3MziQkC', 'yOf6XrrS3uy', 'NKWbkXyfO5F', 'p0NhuIUoeST', 'oVxjBKA1Yzu', 'deKCGAGoEHz', 'S1UMweeoPsi', 'fdBM4sWSuPR', 'SHw2zOysJ1R', 's3iaozBY0dv', 'JP4bMwvJ6oU', 'U5ejGQdX4Ih')));
delete from datavalue where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('NePI9W0zSH5', 'UFlGxXoBrLN', 'ZO0DQJePIEH', 'Psf9CW4BSHM', 'Uc3j0vpsfSB', 'LdQ3CckNpeQ', 'tnek2LjfuIm', 'zna8KfLMXn4', 'XBgvNrxpcDC', 'WHPEpoVDFFv', 'SAV16xEdCZW', 'AAYgHGENgbF', 'ZhvE3MziQkC', 'yOf6XrrS3uy', 'NKWbkXyfO5F', 'p0NhuIUoeST', 'oVxjBKA1Yzu', 'deKCGAGoEHz', 'S1UMweeoPsi', 'fdBM4sWSuPR', 'SHw2zOysJ1R', 's3iaozBY0dv', 'JP4bMwvJ6oU', 'U5ejGQdX4Ih')));
--CSY
--remove all tracker
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('YGa3BmrwwiU'));
delete from programstageinstancecomments where programstageinstanceid in ( select programstageinstanceid from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in ('YGa3BmrwwiU'))));
delete from trackedentityattributevalue where trackedentityinstanceid in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in ('YGa3BmrwwiU')));
delete from trackedentityattributevalueaudit where trackedentityinstanceid in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in ('YGa3BmrwwiU')));
delete from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in ('YGa3BmrwwiU')));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('YGa3BmrwwiU'));
create view tei_to_remove as select trackedentityinstanceid "teiid" from programinstance where programid in (select programid from program where uid in ('YGa3BmrwwiU'));
delete from programinstance where programid in (select programid from program where uid in ('YGa3BmrwwiU'));
delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);
delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);
delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);
drop view tei_to_remove ;
--remove tracker finish
--remove all events
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('auqdJ66DqAT', 'BslciOZ3Rvs', 'gYhhLQlqduE', 'nmXOLkibWJc'));
delete from programstageinstancecomments where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('auqdJ66DqAT', 'BslciOZ3Rvs', 'gYhhLQlqduE', 'nmXOLkibWJc'));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('auqdJ66DqAT', 'BslciOZ3Rvs', 'gYhhLQlqduE', 'nmXOLkibWJc'));
--GHP
--remove all events
delete from trackedentitydatavalueaudit where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('azxjVmQLicj', 'cTzRXZGNvqz'));
delete from programstageinstancecomments where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('azxjVmQLicj', 'cTzRXZGNvqz'));
delete from programstageinstance where programstageinstanceid in ( select psi.programstageinstanceid  from programstageinstance psi inner join programstage ps on ps.programstageid=psi.programstageid inner join program p on p.programid=ps.programid where p.uid in ('azxjVmQLicj', 'cTzRXZGNvqz'));
--remove all datasets
delete from datavalueaudit where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('jfawDJZ5fOX', 'ITKN1N2pPHp', 'KJbR0F9jUbf')));
delete from datavalue where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('jfawDJZ5fOX', 'ITKN1N2pPHp', 'KJbR0F9jUbf')));
--GMP_ENTO
--GMP_EPI
--remove all datasets
delete from datavalueaudit where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('bNJzkNJv0Yg', 'qDd74Hfk2Dc', 'qNtxTrp56wV', 'Cm6vhZilyPa', 'iENTl91i0Hg', 'O34y2Kyxx6P', 'ctDGeFX3fNZ', 'tZyUewB5zBf', 'VEM58nY22sO', 'VTVvoKniNhi', 'zyZLeSbJxqR', 'w9sYDdAm1bx', 'wBSMl47uQib', 'alvBxSxuorK', 'SqN2upmg2Im', 'SqJAH6Pu3Cs', 'Ai48B5k6hwb', 'M05iAqd6ZFU', 'PWCUb3Se1Ie', 'IXuqhJzEYP9', 'I1Tn68LFq5Y')));
delete from datavalue where dataelementid in (select dataelementid from datasetelement where datasetid in (select datasetid from dataset where uid in ('bNJzkNJv0Yg', 'qDd74Hfk2Dc', 'qNtxTrp56wV', 'Cm6vhZilyPa', 'iENTl91i0Hg', 'O34y2Kyxx6P', 'ctDGeFX3fNZ', 'tZyUewB5zBf', 'VEM58nY22sO', 'VTVvoKniNhi', 'zyZLeSbJxqR', 'w9sYDdAm1bx', 'wBSMl47uQib', 'alvBxSxuorK', 'SqN2upmg2Im', 'SqJAH6Pu3Cs', 'Ai48B5k6hwb', 'M05iAqd6ZFU', 'PWCUb3Se1Ie', 'IXuqhJzEYP9', 'I1Tn68LFq5Y')));
--HWF