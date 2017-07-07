using System;

namespace Averitt_RNA.DBAccess
{
    public static class SQLStrings
    {

        private const string DB_DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";

        //GET STAGED ORDERS
        public static string SELECT_STAGED_ORDERS(
            string regionID, string Staged)
        {
            return string.Format(@"
                SELECT *
                FROM [TSDBA].[STAGED_ORDERS]
                WHERE [RegionIdentifier] = '{0}'
                And [Staged} = '{1}'",
                regionID, Staged);
        }

        //GET STAGED SERVICE LOCATIONS
        public static string SELECT_STAGED_SERVICE_LOCATIONS(
                    string regionID, string Status)
        {
            return string.Format(@"
                SELECT *
                FROM [TSDBA].[STAGED_SERVICE_LOCATIONS] 
                WHERE [RegionIdentifier] = '{0}' 
                AND [Status] = '{1}'",
                regionID, Status);
        }

        //GET ALL STAGED SERVICE LOCATIONS
        public static string SELECT_ALL_STAGED_SERVICE_LOCATIONS(
                    string regionID)
        {
            return string.Format(@"
                SELECT *
                FROM [TSDBA].[STAGED_SERVICE_LOCATIONS] 
                WHERE [RegionIdentifier] = '{0}'",
                regionID);
        }

        //INSERT STAGED ROUTES
        public static string INSERT_STAGED_ROUTES(
                    string regionID, string orderId, string routeId, string routeStartTime, string RouteDescr, string stopSeq, string staged, string error, string status)
        {
            return string.Format(@"
                INSERT INTO STAGED_ROUTES
                (RegionIdentifier, OrderIdentifier, RouteIdentifier, RouteStartTime, RouteDescription, StopSequenceNumber, Staged, Error, Status)
                VALUES
                ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", regionID, orderId, routeId, routeStartTime, RouteDescr, stopSeq, staged, error, status);
        }


        public static string DELETE_STAGED_SERVICE_LOCATION(string regionID, string serviceLocationID, string staged, string error, string status)
        {
            return string.Format(@"
               
                SELECT RegionIdentifier, ServiceLocationIdentifier,
                FROM STAGED_SERVICE_LOCATIONS;
                UPDATE STAGED_SERVICE_LOCATIONS
                SET Status = {0}, Error = {1}
                WHERE RegionIdentifier = {2} AND ServiceLocationIdentifier = {3} AND Staged = {4}
                
        
                
                ", status, error, regionID, serviceLocationID, staged);
        }

        public static string UPDATE_STAGED_SERVICE_LOCATION_STATUS(string regionID, string serviceLocationID, string staged, string error, string status)
        {
            return string.Format(@"
               
                SELECT RegionIdentifier, ServiceLocationIdentifier,Staged
                FROM STAGED_SERVICE_LOCATIONS;
                UPDATE STAGED_SERVICE_LOCATIONS
                SET Status = {0}, Error = {1} , 
                WHERE RegionIdentifier = {2} AND ServiceLocationIdentifier = {3} AND Staged = {4}
                ", status, error, regionID, serviceLocationID, staged);
        }

        public static string UPDATE_STAGED_SERVICE_LOCATION(string regionID, string serviceLocationID, string StopSequenceNumber, 
            string routeDescription, string routeStartTime, string routeIdentifier, string orderIdentifier, string staged, string error, string status)
        {
            return string.Format(@"
               
                SELECT RegionIdentifier, ServiceLocationIdentifier,Staged
                FROM STAGED_SERVICE_LOCATIONS;
                UPDATE STAGED_SERVICE_LOCATIONS
                SET Status = {0}, Error = {1}, StopSequenceNumber = {5}, RouteDescription = {6}, RouteStartTime = {7}, RouteIdentifier = {8}, OrderIdentifier = {9}, 
                WHERE RegionIdentifier = {2} AND ServiceLocationIdentifier = {3} AND Staged = {4}
                ", status, error, regionID, serviceLocationID, staged, routeDescription, routeStartTime, routeIdentifier, orderIdentifier);
        }

        public static string UPDATE_STAGED_ORDERS_ERROR(string regionID, string orderId, string staged, string error, string status)
        {
            return string.Format(@"
                UPDATE STAGED_ORDERS
                SET Status = {0}, Error = {1}
                WHERE RegionIdentifier = {2} AND OrderIdentifier = {3} AND Staged = {4}
                ", status, error, regionID, orderId, staged);
        }

        public static string UPDATE_STAGED_ROUTES_ERROR(string regionID, string orderId, string staged, string error, string status)
        {
            return string.Format(@"
                UPDATE STAGED_ROUTES
                SET Status = {0}, Error = {1}
                WHERE RegionIdentifier = {2} AND OrderIdentifier = {3} AND Staged = {4}
                ", status, error, regionID, orderId, staged);
        }


        public static string INSERT_STAGED_ROUTES_UNASSIGNED_ORDER(
                  string regionID, string orderId, string staged, string error, string status)
        {
            return string.Format(@"
                INSERT INTO STAGED_ROUTES
                (RegionIdentifier, OrderIdentifier, RouteIdentifier, RouteStartTime, RouteDescription, StopSequenceNumber, Staged, Error, Status)
                VALUES
                ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", regionID, orderId, null, null, null, null, staged, error, status);
        }
    }
}
