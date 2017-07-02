CREATE TABLE STAGED_ROUTES (
    RegionIdentifier nvarchar(32),
    OrderIdentifier nvarchar(32),
	RouteIdentifier nvarchar(32),
    RouteStartTime datetime2(7),
	RouteDescription nvarchar(255),
	StopSequenceNumber Int,
	Staged datetime2(7),
	Error nvarchar(MAX),
	"Status" nvarchar(50)
);