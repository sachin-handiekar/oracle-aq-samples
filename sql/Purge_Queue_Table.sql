declare
purgeOption dbms_aqadm.aq$_purge_options_t;
begin
purgeOption.block := FALSE;
dbms_aqadm.purge_queue_table(
     queue_table     => 'queue_table',
     purge_condition => NULL,
     purge_options   => purgeOption);
end;