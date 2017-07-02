CREATE TABLE STAGED_SERVICE_LOCATIONS (
    RegionIdentifier nvarchar(32),
    ServiceLocationIdentifier nvarchar(32),
    "Description" nvarchar(255),
	AddressLine1 nvarchar(255),
	AddressLine2 nvarchar(255),
	City nvarchar(255),
	"State" nvarchar(255),
	PostalCode nvarchar(15),
	WorldTimeZone nvarchar(50),
	DeliveryDays nvarchar(7),
	PhoneNumber nvarchar(20),
	ServiceTimeTypeIdentifier nvarchar(32),
	Staged datetime2(7),
	Error nvarchar(MAX),
	"Status" nvarchar(50)
);