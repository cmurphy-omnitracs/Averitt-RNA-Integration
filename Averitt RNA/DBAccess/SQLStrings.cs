using System;

namespace Averitt_RNA.DBAccess
{
    public static class SQLStrings
    {

        private const string DB_DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";

        //GET STAGED ORDERS
        public static string SELECT_STAGED_ORDERS(
            string regionID)
        {
            return string.Format(@"
                SELECT [RegionIdentifier], [OrderIdentifier], [ServiceLocationIdentifier], [BeginDate], [QuantitySize1], [QuantitySize2], [QuantitySize3], [PreferredRouteIdentifier], [OriginDepotIdentifier], [OrderClassIdentifier], [SpecialInstructions], 
                CAST([ServiceWindowOverride1Start] AS varchar) as ServiceWindowOverride1Start , CAST([ServiceWindowOverride1End] AS VARCHAR) as ServiceWindowOverride1End, CAST([ServiceWindowOverride2Start] AS VARCHAR) as ServiceWindowOverride2Start, CAST([ServiceWindowOverride2End] AS VARCHAR) as ServiceWindowOverride2End, 
                [LiftgateOnly], [GuarenteedDelivery] AS GuarenteedDelivery , [Avail], [Delete], [Staged], [Error], [Status] 
                FROM  [STAGED_ORDERS]
                WHERE [RegionIdentifier] = '{0}'
               ",
                regionID);
        }

        //GET STAGED SERVICE LOCATIONS
        public static string SELECT_STAGED_SERVICE_LOCATIONS(
                    string regionID, string Status)
        {
            return string.Format(@"
                SELECT *
                FROM [STAGED_SERVICE_LOCATIONS] 
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
                FROM  [STAGED_SERVICE_LOCATIONS] 
                WHERE [RegionIdentifier] = '{0}'",
                regionID);
        }
        public static string SELECT_ALL_STAGED_SERVICE_LOCATIONS_STATUS(
                 string status)
        {
            return string.Format(@"
                SELECT *
                FROM  [STAGED_SERVICE_LOCATIONS] 
                WHERE [Status] = '{0}'",
                status);
        }

        //INSERT STAGED ROUTES
        public static string INSERT_STAGED_ROUTES(
                    string regionID,  string routeId, string routeStartTime, string RouteDescr, string stopSeq, string staged, string error, string status)
        {
            if (stopSeq == null)
            {
                return string.Format(@"
                INSERT INTO STAGED_ROUTES
                (RegionIdentifier, RouteIdentifier, RouteStartTime, RouteDescription, StopSequenceNumber, Staged, Error, [Status])
                VALUES
                ('{0}', '{1}', CONVERT(datetime2,'{2}'), '{3}', NULL, CONVERT(datetime2,'{5}'), '{6}', '{7}')", regionID, routeId, routeStartTime, RouteDescr, stopSeq, staged, error, status);
            } else
            {
                return string.Format(@"
                INSERT INTO STAGED_ROUTES
                (RegionIdentifier, RouteIdentifier, RouteStartTime, RouteDescription, StopSequenceNumber, Staged, Error, [Status])
                VALUES
                ('{0}', '{1}', CONVERT(datetime2,'{2}'), '{3}', {4}, CONVERT(datetime2,'{5}'), '{6}', '{7}')", regionID, routeId, routeStartTime, RouteDescr, stopSeq, staged, error, status);
            }
        }


        public static string DELETE_EXPIRED_STAGED_SERVICE_LOCATION(string regionID, string serviceLocationID, string staged)
        {
            return string.Format(@"
               
                DELETE FROM STAGED_SERVICE_LOCATIONS
                WHERE RegionIdentifier = '{0}' AND ServiceLocationIdentifier = '{1}' AND Staged =  CONVERT(datetime2,'{2}')
                ", regionID, serviceLocationID, staged);
        }

        public static string UPDATE_STAGED_SERVICE_LOCATION_STATUS(string regionID, string serviceLocationID, string error, string status)
        {
            return string.Format(@"
               
                UPDATE STAGED_SERVICE_LOCATIONS
                SET [Status] = '{0}'
                WHERE RegionIdentifier = '{2}' AND ServiceLocationIdentifier = '{3}'
                ", status, error, regionID, serviceLocationID);
        }

        public static string UPDATE_STAGED_SERVICE_LOCATION_ERROR(string regionID, string serviceLocationID, string staged, string error, string status)
        {
            return string.Format(@"
               
                SELECT RegionIdentifier, ServiceLocationIdentifier,Staged
                FROM STAGED_SERVICE_LOCATIONS;
                UPDATE STAGED_SERVICE_LOCATIONS
                SET [Status] = '{0}', Error = '{1}'
                WHERE RegionIdentifier = '{2}' AND ServiceLocationIdentifier = '{3}' AND Staged =  CONVERT(datetime2,'{4}')
                ", status, error, regionID, serviceLocationID, staged);
        }

        public static string UPDATE_STAGED_SERVICE_LOCATION(string regionID, string serviceLocationID, int StopSequenceNumber,
            string routeDescription, string routeStartTime, string routeIdentifier, string orderIdentifier, string staged, string error, string status)
        {
            return string.Format(@"
               
                SELECT RegionIdentifier, ServiceLocationIdentifier,Staged
                FROM STAGED_SERVICE_LOCATIONS;
                UPDATE STAGED_SERVICE_LOCATIONS
                SET [Status] = '{0}', Error = '{1}', StopSequenceNumber = {5} , RouteDescription = '{6}', RouteStartTime =  CONVERT(datetime2,'{7}'), RouteIdentifier = '{8}', OrderIdentifier = '{9}', 
                WHERE RegionIdentifier = '{2}' AND ServiceLocationIdentifier = '{3}' AND Staged =  CONVERT(datetime2,'{4}')
                ", status, error, regionID, serviceLocationID, staged, routeDescription, routeStartTime, routeIdentifier, orderIdentifier);
        }

        public static string UPDATE_STAGED_ORDERS_STATUS(string regionID, string orderId, string error, string status)
        {
            return string.Format(@"
                UPDATE STAGED_ORDERS
                SET [Status] = '{0}', Error = '{1}'
                WHERE RegionIdentifier = '{2}' AND OrderIdentifier = '{3}'
                ", status, error, regionID, orderId);
        }

        public static string UPDATE_STAGED_ROUTES_ERROR(string regionID, string orderId, string staged, string status, string error)
        {
            return string.Format(@"
                UPDATE STAGED_ROUTES
                SET [Status] = '{0}', Error = '{1}'
                WHERE RegionIdentifier = '{1}' AND OrderIdentifier = '{2}' AND Staged = CONVERT(datetime2,'{4}')
                ", status, error, regionID, orderId, staged);
        }


        public static string INSERT_STAGED_ROUTES_UNASSIGNED_ORDER(
                  string regionID, string orderId, string staged, string status)
        {
            return string.Format(@"
                INSERT INTO STAGED_ROUTES
                (RegionIdentifier, OrderIdentifier, RouteIdentifier, RouteStartTime, RouteDescription, StopSequenceNumber, Staged, [Status])
                VALUES
                ( '{0}', '{1}', '{2}', CONVERT(datetime2,'{3}'), '{4}', {5}, CONVERT(datetime2,'{6}'), '{7}')", regionID, orderId, null, null, null, null, staged, status);
        }

        public static string DELETE_EXPIRED_STAGED_ROUTES(
                  string regionID, string status, string staged, string orderId)
        {
            return string.Format(@"
                DELETE FROM STAGED_ROUTES
                WHERE RegionIdentifier = '{0}' AND [Status] ='{1}'  AND Staged = CAST('{2}' AS datetime2(7)) AND OrderIdentifier = '{3}'
                ", regionID, status, staged, orderId);
        }

        public static string DELETE_EXPIRED_STAGED_ORDERS(string regionID,
                  string orderId, string staged, string status)
        {
            
            return string.Format(@"
                DELETE FROM STAGED_ORDERS
                WHERE [Status] = '{0}' AND RegionIdentifier = '{1}' AND OrderIdentifier = '{2}' AND Staged = CAST('{3}' AS datetime2(7)) 
                ", status, regionID, orderId, staged);
        }

        public static string SELECT_ALL_STAGED_ORDERS_STATUS(string status)
        {
            return string.Format(@"
                SELECT[RegionIdentifier], [OrderIdentifier], [ServiceLocationIdentifier], [BeginDate], [QuantitySize1], [QuantitySize2], [QuantitySize3], [PreferredRouteIdentifier], [OriginDepotIdentifier], [OrderClassIdentifier], [SpecialInstructions],
                CAST([ServiceWindowOverride1Start] AS varchar) as ServiceWindowOverride1Start , CAST([ServiceWindowOverride1End] AS VARCHAR) as ServiceWindowOverride1End, CAST([ServiceWindowOverride2Start] AS VARCHAR) as ServiceWindowOverride2Start, CAST([ServiceWindowOverride2End] AS VARCHAR) as ServiceWindowOverride2End, 
                [LiftgateOnly], [GuarenteedDelivery] AS GuarenteedDelivery, [Avail], [Delete], [Staged], [Error], [Status]
                FROM STAGED_ORDERS
                WHERE [Status] = '{0}'",
                status);
        }

        public static string SELECT_ALL_STAGED_ROUTES_STATUS(string status)
        {
            return string.Format(@"
                SELECT *
                FROM STAGED_ROUTES
                WHERE [Status] = '{0}'",
                status);
        }

        public static string DELETE_DUPLICATE_SERVICE_LOCATIONS()
        {
            return string.Format(@"
                WITH CTE AS(
                SELECT  RegionIdentifier, ServiceLocationIdentifier, Description, AddressLine1, AddressLine2, City, 
                State, PostalCode, WorldTimeZone, DeliveryDays, PhoneNumber, ServiceTimeTypeIdentifier, ServiceWindowTypeIdentifier, [Staged], [Error], [Status], 
                RN = ROW_NUMBER()OVER(PARTITION BY  RegionIdentifier, ServiceLocationIdentifier, Description, AddressLine1, AddressLine2, City, 
                State, PostalCode, WorldTimeZone, DeliveryDays, PhoneNumber, ServiceTimeTypeIdentifier, ServiceWindowTypeIdentifier ORDER BY RegionIdentifier)
                FROM STAGED_SERVICE_LOCATIONS
                )
                DELETE FROM CTE WHERE RN > 1");
        }

        public static string DELETE_DUPLICATE_ORDERS()
        {
            return string.Format(@"
                WITH CTE AS(
                SELECT  RegionIdentifier, OrderIdentifier, ServiceLocationIdentifier, BeginDate, QuantitySize1, QuantitySize2, QuantitySize3, 
                PreferredRouteIdentifier, OriginDepotIdentifier, OrderClassIdentifier, SpecialInstructions, ServiceWindowOverride1Start, ServiceWindowOverride1End, ServiceWindowOverride2Start, 
                ServiceWindowOverride2End, LiftgateOnly, [GuarenteedDelivery] AS GuarenteedDelivery, Avail, [Delete], [Staged], [Error], [Status],
                RN = ROW_NUMBER()OVER(PARTITION BY  RegionIdentifier, OrderIdentifier, ServiceLocationIdentifier, BeginDate, 
                QuantitySize1, QuantitySize2, QuantitySize3, PreferredRouteIdentifier, OriginDepotIdentifier, OrderClassIdentifier, 
                SpecialInstructions, ServiceWindowOverride1Start, ServiceWindowOverride1End, ServiceWindowOverride2Start, ServiceWindowOverride2End, LiftgateOnly, GuarenteedDelivery, 
                Avail, [Delete], [Staged], [Error], [Status] ORDER BY RegionIdentifier)
                FROM STAGED_ORDERS
                )
                DELETE FROM CTE WHERE RN > 1");
        }
    }

}
