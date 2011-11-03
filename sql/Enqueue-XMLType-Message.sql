DECLARE 
 
  queue_options DBMS_AQ.ENQUEUE_OPTIONS_T; 
  message_properties DBMS_AQ.MESSAGE_PROPERTIES_T; 
  message_id RAW(16); 
  message SYS.XMLType; 
 
BEGIN 
 
  message := sys.XMLType.createXML('<sample>hello world</sample>');
  DBMS_AQ.ENQUEUE( queue_name => 'q_sample', 
                   enqueue_options => queue_options, 
                   message_properties => message_properties, 
                   payload => message, 
                   msgid => message_id); 
 
  COMMIT;
 
END;
 