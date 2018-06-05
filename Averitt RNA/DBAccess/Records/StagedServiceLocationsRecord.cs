using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using Averitt_RNA.Apex;
using System.Globalization;

namespace Averitt_RNA.DBAccess.Records
{





    public class StagedServiceLocationRecord : DBAccessUtility.DBRecord, IEquatable<StagedServiceLocationRecord>
    {

        #region Public Properties

        public string RegionIdentifier { get; set; }
        public string ServiceLocationIdentifier { get; set; }
        public string Description { get; set; }
        public string AddressLine1 { get; set; }
        public string AddressLine2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }
        public string WorldTimeZone { get; set; }
        public int DeliveryDays { get; set; }
        public string PhoneNumber { get; set; }
        public string ServiceTimeTypeIdentifier { get; set; }
        public string ServiceWindowTypeIdentifier { get; set; }
        public string Staged { get; set; }
        public string Error { get; set; }
        public string Status { get; set; }

        #endregion

        #region Public Members
        [Flags]
        public enum DeliveryDaysFlag
        {
            Sunday = 1,
            Monday = 2,
            Tuesday = 4,
            Wednesday = 8,
            Thursday = 16,
            Friday = 32,
            Saturday = 64
        }

        public static string TimeFormat = "hh\\:mm\\:ss\\.fffffff";
        #endregion

        #region Public Methods

        override public string ToString()
        {
            // Not sure If i want this. Can't think of a user to have a To String for entire object
            return string.Format("{0} | {1}", RegionIdentifier, ServiceLocationIdentifier);
        }

        override public DBAccessUtility.DBRecord Populate(OdbcDataReader reader)
        {
            return new StagedServiceLocationRecord
            {
                RegionIdentifier = reader["RegionIdentifier"].ToString(),
                ServiceLocationIdentifier = reader["ServiceLocationIdentifier"].ToString(),
                Description = reader["Description"].ToString(),
                AddressLine1 = reader["AddressLine1"].ToString(),
                AddressLine2 = reader["AddressLine2"].ToString(),
                City = reader["City"].ToString(),
                State = reader["State"].ToString(),
                PostalCode = reader["PostalCode"].ToString(),
                WorldTimeZone = reader["WorldTimeZone"].ToString(),
                DeliveryDays = Convert.ToInt32(reader["DeliveryDays"].ToString()),
                PhoneNumber = reader["PhoneNumber"].ToString(),
                ServiceTimeTypeIdentifier = reader["ServiceTimeTypeIdentifier"].ToString(),
                ServiceWindowTypeIdentifier = reader["ServiceWindowTypeIdentifier"].ToString(),
                Staged = reader["Staged"].ToString(),
                Error = reader["Error"].ToString(),
                Status = reader["Status"].ToString()

            } as DBAccessUtility.DBRecord;
        }

       

        #endregion

        #region IEquatable

        public bool Equals(StagedServiceLocationRecord other)
        {
            
            return RegionIdentifier == other.RegionIdentifier &&
                ServiceLocationIdentifier == other.ServiceLocationIdentifier &&
                Description == other.Description &&
                AddressLine1 == other.AddressLine1 &&
                AddressLine2 == other.AddressLine2 &&
                City == other.City &&
                State == other.State &&
                PostalCode == other.PostalCode &&
                WorldTimeZone == other.WorldTimeZone &&
                DeliveryDays == other.DeliveryDays &&
                PhoneNumber == other.PhoneNumber &&
                ServiceTimeTypeIdentifier == other.ServiceTimeTypeIdentifier &&
                ServiceWindowTypeIdentifier == other.ServiceWindowTypeIdentifier;
        }


        public override int GetHashCode()
        {
            
            return StringComparer.InvariantCultureIgnoreCase.GetHashCode(RegionIdentifier + ServiceLocationIdentifier + 
                Description + AddressLine1 + AddressLine2 + City + State + PostalCode + WorldTimeZone + DeliveryDays.ToString() + 
                PhoneNumber + ServiceTimeTypeIdentifier + ServiceWindowTypeIdentifier + Staged + Error + Status);
        }

       

        static public explicit operator ServiceLocation(StagedServiceLocationRecord record)
        {

          
            var mask = (DeliveryDaysFlag)record.DeliveryDays;

            List<DeliveryDaysFlag> result = Enum.GetValues(typeof(DeliveryDaysFlag)).Cast<DeliveryDaysFlag>().Where(value => mask.HasFlag(value)).ToList();
            string combinedDeliveryDay = string.Join(",", result);
          
            
            return new ServiceLocation
            {
                Identifier = record.ServiceLocationIdentifier.ToUpper(),
                Description = record.Description,

                Address = new Address
                {

                    AddressLine1 = record.AddressLine1,
                    AddressLine2 = record.AddressLine2,
                    Locality = new Locality
                    {
                        AdminDivision1 = record.State,
                        AdminDivision3 = record.City,
                        CountryISO3Abbr = "USA",
                        PostalCode = record.PostalCode,
                    },


                },
                Action = ActionType.Add,
                StandingDeliveryQuantities = new Quantities { },
                StandingPickupQuantities = new Quantities { },
                PhoneNumber = record.PhoneNumber,
                WorldTimeZone_TimeZone = record.WorldTimeZone,
                DayOfWeekFlags_DeliveryDays = combinedDeliveryDay,
                
                
            };
                
            
        }


        #endregion


    }
    #region IEqualityComparer
    class ServiceLocationComparer : IEqualityComparer<StagedServiceLocationRecord>
    {
       

        public bool Equals(StagedServiceLocationRecord x, StagedServiceLocationRecord other)
        {

            return x.RegionIdentifier == other.RegionIdentifier &&
                x.ServiceLocationIdentifier == other.ServiceLocationIdentifier &&
                x.Description == other.Description &&
                x.AddressLine1 == other.AddressLine1 &&
                x.AddressLine2 == other.AddressLine2 &&
                x.City == other.City &&
                x.State == other.State &&
                x.PostalCode == other.PostalCode &&
                x.WorldTimeZone == other.WorldTimeZone &&
                x.DeliveryDays == other.DeliveryDays &&
                x.PhoneNumber == other.PhoneNumber &&
                x.ServiceTimeTypeIdentifier == other.ServiceTimeTypeIdentifier &&
                x.ServiceWindowTypeIdentifier == other.ServiceWindowTypeIdentifier;
        }


        public int GetHashCode(StagedServiceLocationRecord location)
        {
            //Check whether the object is null
            if (Object.ReferenceEquals(location, null)) return 0;

            //Get hash code for the regionIdentifier field if it is not null.
            int hashRegionIdentifier = location.RegionIdentifier == null ? 0 : location.RegionIdentifier.GetHashCode();

            //Get hash code for the ServiceLocationIdentifier field if it is not null.
            int hashServiceLocationIdentifier = location.ServiceLocationIdentifier == null ? 0 : location.ServiceLocationIdentifier.GetHashCode();

            //Get hash code for the Description field if it is not null.
            int hashDescription = location.Description == null ? 0 : location.Description.GetHashCode();

            //Get hash code for the AddressLine1 field if it is not null.
            int hashAddressLine1 = location.AddressLine1 == null ? 0 : location.AddressLine1.GetHashCode();

            //Get hash code for the AddressLine2 field if it is not null.
            int hashAddressLine2 = location.AddressLine2 == null ? 0 : location.AddressLine2.GetHashCode();

            //Get hash code for the City field if it is not null.
            int hashCity = location.City == null ? 0 : location.City.GetHashCode();

            //Get hash code for the State field if it is not null.
            int hashState = location.State == null ? 0 : location.State.GetHashCode();

            //Get hash code for the PostalCode field if it is not null.
            int hashPostalCode = location.PostalCode == null ? 0 : location.PostalCode.GetHashCode();

            //Get hash code for the WorldTimeZone field if it is not null.
            int hashWorldTimeZone = location.WorldTimeZone == null ? 0 : location.WorldTimeZone.GetHashCode();

            //Get hash code for the DeliveryDays field if it is not null.
            int hashDeliveryDays = location.DeliveryDays.ToString() == null ? 0 : location.DeliveryDays.ToString().GetHashCode();

            //Get hash code for the PhoneNumber field if it is not null.
            int hashPhoneNumber = location.PhoneNumber == null ? 0 : location.PhoneNumber.GetHashCode();

            //Get hash code for the ServiceTimeTypeIdentifier field if it is not null.
            int hashServiceTimeTypeIdentifier = location.ServiceTimeTypeIdentifier == null ? 0 : location.ServiceTimeTypeIdentifier.GetHashCode();

            //Get hash code for the ServiceWindowTypeIdentifier field if it is not null.
            int hashServiceWindowTypeIdentifier = location.ServiceWindowTypeIdentifier == null ? 0 : location.ServiceWindowTypeIdentifier.GetHashCode();

            return hashRegionIdentifier ^ hashServiceLocationIdentifier ^ hashDescription ^ hashAddressLine1 ^ hashAddressLine2 ^ hashCity ^ hashPostalCode ^ hashState ^ hashPhoneNumber ^ 
                hashServiceTimeTypeIdentifier ^ hashServiceWindowTypeIdentifier ^ hashDeliveryDays ^ hashWorldTimeZone;
        }

 
    }
    #endregion
}
