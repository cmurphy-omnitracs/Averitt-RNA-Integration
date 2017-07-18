using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Averitt_RNA.Apex;

namespace Averitt_RNA
{
    class Testdata
    {
        public static class TestData
        {
            public static ActionType OrderTestMode = ActionType.Add;
            public static string RegionIdentifier = "R1";
            public static string NotificationRecipientIdentifier = "INTEGRATION";
            public static NotificationSubscriptionType[] NotificationSubscriptionTypes = new NotificationSubscriptionType[]
            {
            NotificationSubscriptionType.StopDeparted
            };
            public static DateTime RoutingSessionStartDate = DateTime.Now.AddDays(1).Date;
            public static string DateFormat = "yyyy-MM-dd";
            public static string TimeFormat = "hh\\:mm\\:ss\\.fffffff";
            public static Address Address = new Address
            {
                AddressLine1 = "849 Fairmount Ave",
                AddressLine2 = "#500",
                Locality = new Locality
                {
                    AdminDivision1 = "MD", // State
                    AdminDivision3 = "Towson", // City
                    CountryISO3Abbr = "USA",
                    PostalCode = "21286"
                }
            };
            public static ServiceLocation ServiceLocation = new ServiceLocation
            {
                Action = ActionType.Add,
                Identifier = "TESTLOC1",
                Description = "Roadnet",
                DayOfWeekFlags_DeliveryDays = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Monday, DayOfWeekFlags.Tuesday, DayOfWeekFlags.Wednesday, DayOfWeekFlags.Thursday, DayOfWeekFlags.Friday }),
                Address = Address,
                PhoneNumber = "(410) 847-1900",
                ServiceTimeTypeEntityKey = 101,
                TimeWindowTypeEntityKey = 101,
                //OpenCloseOverrides = new ServiceLocationOpenCloseDetail[]
                //{
                //new ServiceLocationOpenCloseDetail
                //{
                //    Action = ActionType.Add,
                //    DailyTimePeriod = new DailyTimePeriod
                //    {
                //        DayOfWeekFlags_DaysOfWeek = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Monday, DayOfWeekFlags.Wednesday, DayOfWeekFlags.Friday }),
                //        EndTime = new TimeSpan(17, 0, 0).ToString(TimeFormat),
                //        StartTime = new TimeSpan(9, 0, 0).ToString(TimeFormat)
                //    },
                //    OrderClassEntityKey = 101
                //},
                //new ServiceLocationOpenCloseDetail
                //{
                //    Action = ActionType.Add,
                //    DailyTimePeriod = new DailyTimePeriod
                //    {
                //        DayOfWeekFlags_DaysOfWeek = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Tuesday, DayOfWeekFlags.Thursday }),
                //        EndTime = new TimeSpan(20, 0, 0).ToString(TimeFormat),
                //        StartTime = new TimeSpan(9, 0, 0).ToString(TimeFormat)
                //    },
                //    OrderClassEntityKey = 101
                //}
                //},
                //ServiceWindowOverrides = new ServiceLocationServiceWindowDetail[]
                //{
                //new ServiceLocationServiceWindowDetail
                //{
                //    Action = ActionType.Add,
                //    DailyTimePeriod = new DailyTimePeriod
                //    {
                //        DayOfWeekFlags_DaysOfWeek = DayOfWeekFlags.WeekDays.ToString(),
                //        EndTime = new TimeSpan(12, 0, 0).ToString(TimeFormat),
                //        StartTime = new TimeSpan(9, 0, 0).ToString(TimeFormat)
                //    },
                //    OrderClassEntityKey = 101
                //},
                //new ServiceLocationServiceWindowDetail
                //{
                //    Action = ActionType.Add,
                //    DailyTimePeriod = new DailyTimePeriod
                //    {
                //        DayOfWeekFlags_DaysOfWeek = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Monday, DayOfWeekFlags.Wednesday, DayOfWeekFlags.Friday }),
                //        EndTime = new TimeSpan(17, 0, 0).ToString(TimeFormat),
                //        StartTime = new TimeSpan(13, 0, 0).ToString(TimeFormat)
                //    },
                //    OrderClassEntityKey = 101
                //},
                //new ServiceLocationServiceWindowDetail
                //{
                //    Action = ActionType.Add,
                //    DailyTimePeriod = new DailyTimePeriod
                //    {
                //        DayOfWeekFlags_DaysOfWeek = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Tuesday, DayOfWeekFlags.Thursday }),
                //        EndTime = new TimeSpan(20, 0, 0).ToString(TimeFormat),
                //        StartTime = new TimeSpan(13, 0, 0).ToString(TimeFormat)
                //    },
                //    OrderClassEntityKey = 101
                //}
                //},
                //StandingDeliveryQuantities = new Quantities { },
                //StandingPickupQuantities = new Quantities { },
                //WorldTimeZone_TimeZone = Enum.GetName(typeof(WorldTimeZone), WorldTimeZone.EasternTimeUSCanada)
            };
            public static DailyRoutingSession DailyRoutingSession = new DailyRoutingSession
            {
                Action = ActionType.Add,
                StartDate = RoutingSessionStartDate.ToString(DateFormat),
                SessionMode_Mode = Enum.GetName(typeof(SessionMode), SessionMode.Operational),
                Description = "Test Session",
                TimeUnit_TimeUnitType = Enum.GetName(typeof(TimeUnit), TimeUnit.Day),
                NumberOfTimeUnits = 1
            };
            public static OrderSpec OrderSpec = new OrderSpec
            {
                OrderClassEntityKey = 101,
                Identifier = "TESTORDER1",
                BeginDate = RoutingSessionStartDate.ToString(DateFormat),
                EndDate = RoutingSessionStartDate.ToString(DateFormat),
                LineItems = new LineItem[]
                {
                new LineItem
                {
                    Identifier = "LINEITEM1",
                    LineItemType_Type = Enum.GetName(typeof(LineItemType), LineItemType.Delivery),
                    Quantities = new Quantities
                    {
                        Size1 = 50.0
                    }
                },
                new LineItem
                {
                    Identifier = "LINEITEM2",
                    LineItemType_Type = Enum.GetName(typeof(LineItemType), LineItemType.Delivery),
                    Quantities = new Quantities
                    {
                        Size1 = 50.0
                    }
                }
                }
            };
            public static TaskOpenCloseOverrideDetail TaskOpenCloseOverrideDetail = new TaskOpenCloseOverrideDetail
            {
                Action = ActionType.Add,
                DailyTimePeriod = new DailyTimePeriod
                {
                    DayOfWeekFlags_DaysOfWeek = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Monday, DayOfWeekFlags.Wednesday, DayOfWeekFlags.Friday }),
                    StartTime = new TimeSpan(9, 0, 0).ToString(TimeFormat),
                    EndTime = new TimeSpan(17, 0, 0).ToString(TimeFormat)
                }
            };
            public static TaskServiceWindowOverrideDetail TaskServiceWindowOverrideDetail = new TaskServiceWindowOverrideDetail
            {
                Action = ActionType.Add,
                DailyTimePeriod = new DailyTimePeriod
                {
                    DayOfWeekFlags_DaysOfWeek = string.Join(",", new DayOfWeekFlags[] { DayOfWeekFlags.Monday, DayOfWeekFlags.Wednesday, DayOfWeekFlags.Friday }),
                    StartTime = new TimeSpan(9, 0, 0).ToString(TimeFormat),
                    EndTime = new TimeSpan(17, 0, 0).ToString(TimeFormat)
                }
            };
        }
    }
}
