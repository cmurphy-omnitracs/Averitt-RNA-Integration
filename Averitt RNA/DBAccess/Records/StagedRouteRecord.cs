using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using Averitt_RNA.Apex;
using Averitt_RNA;

namespace Averitt_RNA.DBAccess.Records
{
    public class StagedRouteRecord : DBAccessUtility.DBRecord, IEquatable<StagedRouteRecord>
    {

        #region Public Properties

        public string RegionIdentifier { get; set; }
        public string OrderIdentifier { get; set; }
        public string RouteIdentifier { get; set; }
        public string RouteStartTime { get; set; }
        public string RouteDescription { get; set; }
        public string StopSequenceNumber { get; set; }
        public string Staged { get; set; }
        public string Error { get; set; }
        public string Status { get; set; }

        #endregion

        #region Public Methods

        override public string ToString()
        {
            // Not sure If i want this. Can't think of a user to have a To String for entire object
            return string.Format("{0} | {1}", RegionIdentifier, OrderIdentifier);
        }

        override public DBAccessUtility.DBRecord Populate(OdbcDataReader reader)
        {
            return new StagedRouteRecord
            {
                RegionIdentifier = reader["RegionIdentifier"].ToString(),
                OrderIdentifier = reader["OrderIdentifier"].ToString(),
                RouteIdentifier = reader["RouteIdentifier"].ToString(),
                RouteStartTime = reader["RouteStartTime"].ToString(),
                RouteDescription = reader["RouteDescription"].ToString(),
                StopSequenceNumber = reader["StopSequenceNumber"].ToString(),
                Staged = reader["Staged"].ToString(),
                Error = reader["Error"].ToString(),
                Status = reader["Status"].ToString()

            } as DBAccessUtility.DBRecord;
        }

       

        #endregion

        #region IEquatable

        public bool Equals(StagedRouteRecord other)
        {
            //TODO
            return RegionIdentifier == other.RegionIdentifier
                && OrderIdentifier == other.OrderIdentifier
                && RouteIdentifier == other.RouteIdentifier
                && RouteStartTime == other.RouteStartTime
                && RouteDescription == other.RouteDescription
                && StopSequenceNumber == other.StopSequenceNumber
                && Staged == other.Staged
                && Error == other.Error
                && Status == other.Status;
           
        }

        public override int GetHashCode()
        {
            
            return StringComparer.InvariantCultureIgnoreCase.GetHashCode(RegionIdentifier + OrderIdentifier + RouteIdentifier +
                                    RouteStartTime + RouteDescription + StopSequenceNumber + Staged + Error + Status);
        }

        static public explicit operator Route(StagedRouteRecord record)
        {
           
            ServiceableStop[] stop = new ServiceableStop[] { };
            StopAction[] orders = new StopAction[] { };
            stop[0].SequenceNumber = Convert.ToInt32(record.StopSequenceNumber);
            orders[0].OrderIdentifier = record.StopSequenceNumber;
            stop[0].Actions = orders;
            DateTime startTime;
            DateTime.TryParse(record.RouteStartTime, out startTime);

            

            return new Route
            {
                Identifier = record.RouteIdentifier,
                Stops = stop,
                Description = record.RouteDescription,
                StartTime = new QualityPairedDateTime
                {
                    Value = startTime
                }
                                

            };


        }

        #endregion

    }
}
