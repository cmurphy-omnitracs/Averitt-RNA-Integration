using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using Averitt_RNA.Apex;
using System.Data.SqlTypes;

namespace Averitt_RNA.DBAccess.Records
{
    public class StagedOrderRecord : DBAccessUtility.DBRecord, IEquatable<StagedOrderRecord>
    {

        #region Public Properties

        public string RegionIdentifier { get; set; }
        public string OrderIdentifier { get; set; }
        public string ServiceLocationIdentifier { get; set; }
        public string BeginDate { get; set; }
        public string QuantitySize1 { get; set; }
        public string QuantitySize2 { get; set; }
        public string QuantitySize3 { get; set; }
        public string PreferredRouteIdentifier { get; set; }
        public string OriginDepotIdentifier { get; set; }
        public string OrderClassIdentifier { get; set; }
        public string SpecialInstructions { get; set; }
        public string ServiceWindowOverride1Start { get; set; }
        public string ServiceWindowOverride1End { get; set; }
        public string ServiceWindowOverride2Start { get; set; }
        public string ServiceWindowOverride2End { get; set; }
        public string LiftgateOnly { get; set; }
        public string GuaranteedDelivery { get; set; }
        public string Avail { get; set; }
        public SqlBoolean Delete { get; set; }
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

            DateTime test = (DateTime)reader["BeginDate"];
            string beginDate = test.ToString("yyyy-MM-dd");
            return new StagedOrderRecord
            {
                RegionIdentifier = reader["RegionIdentifier"].ToString(),
                OrderIdentifier = reader["OrderIdentifier"].ToString(),
                ServiceLocationIdentifier = reader["ServiceLocationIdentifier"].ToString(),
                BeginDate = beginDate,
                QuantitySize1 = reader["QuantitySize1"].ToString(),
                QuantitySize2 = reader["QuantitySize2"].ToString(),
                QuantitySize3 = reader["QuantitySize3"].ToString(),
                PreferredRouteIdentifier = reader["PreferredRouteIdentifier"].ToString(),
                OriginDepotIdentifier = reader["OriginDepotIdentifier"].ToString(),
                OrderClassIdentifier = reader["OrderClassIdentifier"].ToString(),
                SpecialInstructions = reader["SpecialInstructions"].ToString(),
                ServiceWindowOverride1Start = (reader["ServiceWindowOverride1Start"]).ToString(),
                ServiceWindowOverride1End = (reader["ServiceWindowOverride1End"]).ToString(),
                ServiceWindowOverride2Start = (reader["ServiceWindowOverride2Start"]).ToString(),
                ServiceWindowOverride2End = (reader["ServiceWindowOverride2End"]).ToString(),
                LiftgateOnly = reader["LiftgateOnly"].ToString(),
                GuaranteedDelivery = reader["GuaranteedDelivery"].ToString(),
                Avail = reader["Avail"].ToString(),
                Delete = Convert.ToBoolean(reader["Delete"]),
                Staged = reader["Staged"].ToString(),
                Error = reader["Error"].ToString(),
                Status = reader["Status"].ToString()

            } as DBAccessUtility.DBRecord;
        }


        public OrderSpec ToOrderSpec()
        {
            return new OrderSpec
            {

            };
        }

        #endregion

        #region IEquatable

        public bool Equals(StagedOrderRecord other)
        {
            bool status = RegionIdentifier == other.RegionIdentifier
                && ServiceLocationIdentifier == other.ServiceLocationIdentifier
                && OrderIdentifier == other.OrderIdentifier
                && BeginDate == other.BeginDate
                && QuantitySize1 == other.QuantitySize1
                && QuantitySize2 == other.QuantitySize2
                && QuantitySize3 == other.QuantitySize3
                && PreferredRouteIdentifier == other.PreferredRouteIdentifier
                && OriginDepotIdentifier == other.OriginDepotIdentifier
                && OrderClassIdentifier == other.OrderClassIdentifier
                && SpecialInstructions == other.SpecialInstructions
                && ServiceWindowOverride1Start == other.ServiceWindowOverride1Start
                && ServiceWindowOverride1End == other.ServiceWindowOverride1End
                && ServiceWindowOverride2Start == other.ServiceWindowOverride2Start
                && ServiceWindowOverride2End == other.ServiceWindowOverride2End
                && LiftgateOnly == other.LiftgateOnly
                && GuaranteedDelivery == other.GuaranteedDelivery
                && Avail == other.Avail
                && (bool)Delete == (bool)other.Delete
                && Staged == other.Staged
                && Error == other.Error
                && Status == other.Status;
            return status;


        }

        public override int GetHashCode()
        {

            return StringComparer.InvariantCultureIgnoreCase.GetHashCode(RegionIdentifier + OrderIdentifier + ServiceLocationIdentifier +
                                    BeginDate + QuantitySize1 + QuantitySize2 + QuantitySize3 + PreferredRouteIdentifier + OriginDepotIdentifier +
                                    OrderClassIdentifier + SpecialInstructions + ServiceWindowOverride1Start + ServiceWindowOverride1End +
                                    ServiceWindowOverride2Start + ServiceWindowOverride2End + LiftgateOnly + GuaranteedDelivery + Avail +
                                    Delete + Staged + Error + Status);
        }

        static public explicit operator Order(StagedOrderRecord record)
        {

            Dictionary<string, object> dict = new Dictionary<string, object>();
            dict.Add("LiftGateOnly", record.LiftgateOnly);
            dict.Add("GuaranteedDelivery", record.GuaranteedDelivery);
            dict.Add("Avail", record.Avail);

            TaskServiceWindowOverrideDetail tempServiceWindowOverride = new TaskServiceWindowOverrideDetail();
            TaskServiceWindowOverrideDetail temp2ServiceWindowOverride = new TaskServiceWindowOverrideDetail();
         
            if ((record.ServiceWindowOverride1Start != null && record.ServiceWindowOverride1End != null) && (record.ServiceWindowOverride1Start != string.Empty
                && record.ServiceWindowOverride1End != string.Empty))
            {
                tempServiceWindowOverride = new TaskServiceWindowOverrideDetail
                {
                    Action = ActionType.Add,
                    DailyTimePeriod = new DailyTimePeriod
                    {
                        StartTime = record.ServiceWindowOverride1Start,
                        EndTime = record.ServiceWindowOverride1End,
                        DayOfWeekFlags_DaysOfWeek = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday"
                    }
                };

            }
            else
            {
                tempServiceWindowOverride = null;

            }
            if ((record.ServiceWindowOverride2Start != null && record.ServiceWindowOverride2End != null) && (record.ServiceWindowOverride2Start != string.Empty
                && record.ServiceWindowOverride2End != string.Empty))
            {


                temp2ServiceWindowOverride = new TaskServiceWindowOverrideDetail
                {
                    Action = ActionType.Add,
                    DailyTimePeriod = new DailyTimePeriod
                    {
                        StartTime = record.ServiceWindowOverride2Start,
                        EndTime = record.ServiceWindowOverride2End,
                        DayOfWeekFlags_DaysOfWeek = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday"
                    }
                };

            }
            else
            {
                temp2ServiceWindowOverride = null;

            }
            TaskServiceWindowOverrideDetail[] serviceWindowOverride = new TaskServiceWindowOverrideDetail[] { };
            if (tempServiceWindowOverride != null)
            {
                if (temp2ServiceWindowOverride != null)
                {
                    serviceWindowOverride = new TaskServiceWindowOverrideDetail[] { tempServiceWindowOverride, temp2ServiceWindowOverride };
                }
                else
                {
                    serviceWindowOverride = new TaskServiceWindowOverrideDetail[] { tempServiceWindowOverride };
                }

            }



            Task[] task = new Task[] {
                new Task {
                    TaskType_Type = "Delivery",
                    LocationIdentifier = record.ServiceLocationIdentifier,
                    Quantities = new Quantities
                    {
                        Size1 = Convert.ToDouble(record.QuantitySize1),
                        Size2 = Convert.ToDouble(record.QuantitySize2),
                        Size3 = Convert.ToDouble(record.QuantitySize3)

                    },
                    ServiceWindowOverrides = serviceWindowOverride,
                    

                } };

            Order order = new Order
            {
                
                Identifier = record.OrderIdentifier,
                BeginDate = record.BeginDate,
                PlannedDeliveryQuantities = new Quantities
                {
                    Size1 = Convert.ToDouble(record.QuantitySize1),
                    Size2 = Convert.ToDouble(record.QuantitySize2),
                    Size3 = Convert.ToDouble(record.QuantitySize3)

                },
                PreferredRouteIdentifier = record.PreferredRouteIdentifier,
                SpecialInstructions = record.SpecialInstructions,
                CustomProperties = dict,
                Tasks = task,


            };

            if (record.Delete == true)
            {
                order.Action = ActionType.Delete;
            }

            return order;





        }

     



        #endregion

    }

    #region IEqualityComparer
    class OrderComparer : IEqualityComparer<StagedOrderRecord>
    {


        public bool Equals(StagedOrderRecord x, StagedOrderRecord other)
        {

            return x.RegionIdentifier == other.RegionIdentifier &&
                x.ServiceLocationIdentifier == other.ServiceLocationIdentifier &&
                x.OrderIdentifier == other.OrderIdentifier &&
                x.OrderClassIdentifier == other.OrderClassIdentifier &&
                x.BeginDate == other.BeginDate &&
                x.QuantitySize1 == other.QuantitySize1 &&
                x.QuantitySize2 == other.QuantitySize2 &&
                x.QuantitySize3 == other.QuantitySize3 &&
                x.PreferredRouteIdentifier == other.PreferredRouteIdentifier &&
                x.OriginDepotIdentifier == other.OriginDepotIdentifier &&
                (bool)x.Delete == (bool)other.Delete &&
                x.Status == other.Status;
        }

        public bool Equals(StagedOrderRecord x, Order other, DictCache dictCache)
        {

            return x.ServiceLocationIdentifier == other.Tasks[0].LocationIdentifier &&
                x.OrderIdentifier == other.Identifier &&
                x.BeginDate == other.BeginDate &&
                x.OrderClassIdentifier == dictCache.orderClassesDict.Where(orderclass => orderclass.Value == other.OrderClassEntityKey).Select(orderclass => orderclass.Key).FirstOrDefault(null) &&
                x.RegionIdentifier == MainService.Regions.Where(region => region.EntityKey == other.RegionEntityKey).Select(region => region.Identifier).FirstOrDefault(null);

        }

        public int GetHashCode(StagedOrderRecord order)
        {
            //Check whether the object is null
            if (Object.ReferenceEquals(order, null)) return 0;

            //Get hash code for the regionIdentifier field if it is not null.
            int hashRegionIdentifier = order.RegionIdentifier == null ? 0 : order.RegionIdentifier.GetHashCode();

            //Get hash code for the ServiceLocationIdentifier field if it is not null.
            int hashServiceLocationIdentifier = order.ServiceLocationIdentifier == null ? 0 : order.ServiceLocationIdentifier.GetHashCode();

            //Get hash code for the OrderIdentifier field if it is not null.
            int hashOrderIdentifier = order.OrderIdentifier == null ? 0 : order.OrderIdentifier.GetHashCode();

            //Get hash code for the OrderClassIdentifier field if it is not null.
            int hashOrderClassIdentifier = order.OrderClassIdentifier == null ? 0 : order.OrderClassIdentifier.GetHashCode();

            //Get hash code for the BeginDate field if it is not null.
            int hashBeginDate = order.BeginDate == null ? 0 : order.BeginDate.GetHashCode();

            //Get hash code for the QuantitySize1 field if it is not null.
            int hashQuantitySize1 = order.QuantitySize1 == null ? 0 : order.QuantitySize1.GetHashCode();

            //Get hash code for the QuantitySize2 field if it is not null.
            int hashQuantitySize2 = order.QuantitySize2 == null ? 0 : order.QuantitySize2.GetHashCode();

            //Get hash code for the QuantitySize3 field if it is not null.
            int hashQuantitySize3 = order.QuantitySize3 == null ? 0 : order.QuantitySize3.GetHashCode();

            //Get hash code for the PreferredRouteIdentifier field if it is not null.
            int hashPreferredRouteIdentifier = order.PreferredRouteIdentifier == null ? 0 : order.PreferredRouteIdentifier.GetHashCode();

            //Get hash code for the Delete field if it is not null.
            int hashDelete = order.Delete.ToString() == null ? 0 : order.Delete.ToString().GetHashCode();

            //Get hash code for the OriginDepotIdentifier field if it is not null.
            int hashOriginDepotIdentifier = order.OriginDepotIdentifier == null ? 0 : order.OriginDepotIdentifier.GetHashCode();

            //Get hash code for the Status field if it is not null.
            int hashStatus = order.Status == null ? 0 : order.Status.GetHashCode();


            return hashRegionIdentifier ^ hashServiceLocationIdentifier ^ hashOrderIdentifier ^ hashOrderClassIdentifier ^ hashBeginDate ^
                hashQuantitySize1 ^ hashQuantitySize2 ^ hashQuantitySize3 ^ hashPreferredRouteIdentifier ^ hashDelete ^ hashOriginDepotIdentifier ^ hashStatus;
        }


    }
    #endregion
}
