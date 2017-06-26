using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using Averitt_RNA.Apex;

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
        public char Delete { get; set; }
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
            return new StagedOrderRecord
            {
                RegionIdentifier = reader["RegionIdentifier"].ToString(),
                OrderIdentifier = reader["OrderIdentifier"].ToString(),
                ServiceLocationIdentifier = reader["ServiceLocationIdentifier"].ToString(),
                BeginDate = reader["BeginDate"].ToString(),
                QuantitySize1 = reader["QuantitySize1"].ToString(),
                QuantitySize2 = reader["QuantitySize2"].ToString(),
                QuantitySize3 = reader["QuantitySize3"].ToString(),
                PreferredRouteIdentifier = reader["PreferredRouteIdentifier"].ToString(),
                OriginDepotIdentifier = reader["OriginDepotIdentifier"].ToString(),
                OrderClassIdentifier = reader["OrderClassIdentifier"].ToString(),
                SpecialInstructions = reader["SpecialInstructions"].ToString(),
                ServiceWindowOverride1Start = reader["ServiceWindowOverride1Start"].ToString(),
                ServiceWindowOverride1End = reader["ServiceWindowOverride1End"].ToString(),
                ServiceWindowOverride2Start = reader["ServiceWindowOverride2Start"].ToString(),
                ServiceWindowOverride2End = reader["ServiceWindowOverride2End"].ToString(),
                LiftgateOnly = reader["LiftgateOnly"].ToString(),
                GuaranteedDelivery = reader["GuaranteedDelivery"].ToString(),
                Avail = reader["Avail"].ToString(),
                Delete = Convert.ToChar(reader["Delete"]),
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
            //TODO
            return RegionIdentifier == other.RegionIdentifier
                && ServiceLocationIdentifier == other.ServiceLocationIdentifier
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
                && Delete == other.Delete
                && Staged == other.Staged
                && Error == other.Error
                && Status == other.Status;
        }

        public override int GetHashCode()
        {
            //TODO
            return StringComparer.InvariantCultureIgnoreCase.GetHashCode(RegionIdentifier + OrderIdentifier + ServiceLocationIdentifier +
                                    BeginDate + QuantitySize1 + QuantitySize2 + QuantitySize3 + PreferredRouteIdentifier + OriginDepotIdentifier +
                                    OrderClassIdentifier + SpecialInstructions + ServiceWindowOverride1Start + ServiceWindowOverride1End +
                                    ServiceWindowOverride2Start + ServiceWindowOverride2End + LiftgateOnly + GuaranteedDelivery + Avail +
                                    Delete + Staged + Error + Status);
        }

        #endregion

    }
}