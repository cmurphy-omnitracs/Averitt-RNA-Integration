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
                ServiceWindowTypeIdentifier == other.ServiceWindowTypeIdentifier &&
                Staged == other.Staged &&
                Error == other.Error &&
                Status == other.Status;
        }

        public override int GetHashCode()
        {
            
            return StringComparer.InvariantCultureIgnoreCase.GetHashCode(RegionIdentifier + ServiceLocationIdentifier + 
                Description + AddressLine1 + AddressLine2 + City + State + PostalCode + WorldTimeZone + DeliveryDays.ToString() + 
                PhoneNumber + ServiceTimeTypeIdentifier + ServiceWindowTypeIdentifier + Staged + Error + Status);
        }

       

       

        static public explicit operator ServiceLocation(StagedServiceLocationRecord record)
        {

           string dayOfTheWeek = string.Empty;

            
            var temp = Convert.ToString(record.DeliveryDays,2).Reverse().ToArray();
            
            if(temp[0] == '1')
            {
                dayOfTheWeek.Insert(0, "S");
            } else if(temp[1] == '1')
            {
                dayOfTheWeek.Insert(1, "M");
            }
            else if (temp[2] == '1')
            {
                dayOfTheWeek.Insert(2, "T");
            }else if(temp[3] == '1')
            {
                dayOfTheWeek.Insert(3, "W");
            }else if(temp[4] == '1')
            {
                dayOfTheWeek.Insert(4, "Th");
            }else if(temp[5] == '1')
            {
                dayOfTheWeek.Insert(5, "F");
            }
            else if (temp[6] == '1')
            {
                dayOfTheWeek.Insert(6, "Sa");
            }


            
                return new ServiceLocation
            {
                Identifier = record.ServiceLocationIdentifier,
                Description = record.Description,
                Address = new Address
                {
                    AddressLine1 = record.AddressLine1,
                    AddressLine2 = record.AddressLine2,
                    Locality = new Locality
                    {
                        PostalCode = record.PostalCode,
                    },
                  
                    
                },
                   
                    
                PhoneNumber = record.PhoneNumber,
                WorldTimeZone_TimeZone = record.WorldTimeZone,
                DayOfWeekFlags_DeliveryDays = dayOfTheWeek,
                

                
               
                
                
                

            };
                
            
        }


        #endregion
    }
}
