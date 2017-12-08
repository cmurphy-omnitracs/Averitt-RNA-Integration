GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC1','Test service location 1 duplicate','1201 S. Joyce Street','Apt 201','Arlington','Virginia','22202','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'NEW')

GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC1','Test service location 1 duplicate','1201 S. Joyce Street','Apt 201','Arlington','Virginia','22202','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'NEW')

GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC3','Test service location 3 COMPLETE','2400 24th Rd. S','Apt 201','Arlington','Virginia','22206','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'COMPLETE')

GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC4','Test service location 4 ERROR','3211 Wilson Blvd.','','Arlington','Virginia','22201','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'ERROR')

GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC5','Test service location 5 NEW','550 N Quincy St','','Arlington','Virginia','22203','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'ERROR')


GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R2','SRVC_LOC6','Test service location 6 NEW Different Region','1500 Wilson Blvd','102102','Arlington','Virginia','22209','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'ERROR')

GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC7','Test service location SHOULD NOT BE IN RNA','2700 Wilson Blvd','','Arlington','Virginia','22201','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'ERROR')


GO
INSERT INTO STAGED_SERVICE_LOCATIONS
([RegionIdentifier],[ServiceLocationIdentifier],[Description],[AddressLine1],[AddressLine2],[City],[State],[PostalCode],[WorldTimeZone], [DeliveryDays],
[PhoneNumber],[ServiceTimeTypeIdentifier],[ServiceWindowTypeIdentifier],[Staged],[Error],[Status])                              
VALUES
('R1','SRVC_LOC8','Test service location blank address information fields','','','','','','EasternTimeUSCanada',42,'3402448442','DEFAULT_STT','DEFAULT_SWT','DATETIME',' ', 'ERROR')

