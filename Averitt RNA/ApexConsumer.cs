using System;
using System.Linq;
using System.ServiceModel;
using Averitt_RNA.Apex;
using System.Collections.Generic;
using System.Collections;
using System.IO;
using System.ComponentModel;
using System.Reflection;
using System.Data.SqlTypes;
using MoreLinq;




namespace Averitt_RNA
{
    public class ApexConsumer
    {

        #region Private Members

        private long _BusinessUnitEntityKey;
        private Region _Region;
        private RegionContext _RegionContext;
        private log4net.ILog _Logger;
        private QueryServiceClient _QueryServiceClient;
        private MappingServiceClient _MappingServiceClient;
        private RoutingServiceClient _RoutingServiceClient;
        private Address test123 = Testdata.TestData.ServiceLocation.Address;

        #endregion



        #region Public Members

        public enum ErrorLevel
        {
            None,
            Transient,
            Partial,
            Fatal
        }
        public enum TaskSpecType
        {
            None,
            Delivery,
            Pickup,
            DeliveryAndPickup,
            Transfer
        }
        public enum RetrieveType
        {
            CreateRoutesJobInfo,
            CustomForm,
            CustomFormResponse,
            DailyPass,
            DailyRoutingSession,
            Depot,
            Equipment,
            EquipmentManufacturer,
            EquipmentType,
            FormControl,
            MobileDevice,
            Notification,
            NotificationRecipient,
            Order,
            OrderClass,
            PackageType,
            Region,
            Route,
            ServiceLocation,
            ServiceTimeType,
            SKU,
            TelematicsDevice,
            TimeWindowType,
            UnassignedOrderGroup,
            Worker,
            WorkerType,
            DailyPassTemplate
        }
        public Dictionary<int, string> GeocodeAccuracyDict = new Dictionary<int, string>
        {

            { 0,"StreetExact"},
            {1, "RooftopExact"},
            { 2,"StreetHigh"},
            { 3, "RooftopHigh"},
            {4, "StreetMedium"},
            {5, "RooftopMedium"},
            {6, "StreetLow"},
            { 7, "RooftopLow"},
            { 8, "PostalDetail"},
            { 9, "Postal"},
            { 10, "City"},
            {11, "Not Applicable" },
        };


        public const string DATE_FORMAT = "yyyy-MM-dd";
        public const string DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";
        public const string TIMESPAN_FORMAT = "hh\\:mm\\:ss\\.fffffff";
        public const string TRUNCATED_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
        public const string CANADA_COUNTRY_ISO3_ABBR = "CAN";
        public const string MEXICO_COUNTRY_ISO3_ABBR = "MEX";
        public const string USA_COUNTRY_ISO3_ABBR = "USA";
        public const string NOTIFICATION_RECIPIENT_IDENTIFIER = "INTEGRATION";
        public const int BATCH_SIZE = 100;
        public const string DEFAULT_IDENTIFIER = "DEFAULT";
        public const int MAX_GEOCODE_RETRIES = 5;

        #endregion

        #region Public Static Methods

        public static string GenerateUniqueIdentifier()
        {
            return Guid.NewGuid().ToString().Replace("-", string.Empty).ToUpper();
        }

        public static OrderSpec ConvertOrderToOrderSpec(Order order)
        {
            OrderSpec orderSpec = new OrderSpec
            {
                BeginDate = order.BeginDate,
                CustomProperties = order.CustomProperties,
                EndDate = order.EndDate,
                ForceBulkServiceTime = order.ForceBulkServiceTime,
                Identifier = order.Identifier,
                LineItems = order.LineItems,
                ManagedByUserEntityKey = order.ManagedByUserEntityKey,
                NetRevenue = order.NetRevenue,
                OrderClassEntityKey = order.OrderClassEntityKey,
                OrderInstance = new DomainInstance
                {
                    EntityKey = order.EntityKey,
                    Version = order.Version
                },
                PlannedDeliveryCategory1Quantities = order.PlannedDeliveryCategory1Quantities,
                PlannedDeliveryCategory2Quantities = order.PlannedDeliveryCategory2Quantities,
                PlannedDeliveryCategory3Quantities = order.PlannedDeliveryCategory3Quantities,
                PlannedPickupCategory1Quantities = order.PlannedPickupCategory1Quantities,
                PlannedPickupCategory2Quantities = order.PlannedPickupCategory2Quantities,
                PlannedPickupCategory3Quantities = order.PlannedPickupCategory3Quantities,
                PreferredRouteIdentifierOverride = order.PreferredRouteIdentifierOverride,
                RegionEntityKey = order.RegionEntityKey,
                Selector = order.Selector,
                SessionEntityKey = order.SessionEntityKey,
                SpecialInstructions = order.SpecialInstructions,
                TakenBy = order.TakenBy,
                UploadSelector = order.UploadSelector

            };
            TaskSpecType taskSpecType = TaskSpecType.None;
            if (order.LineItems == null || order.LineItems.Length == 0)
            {
                if (order.Tasks != null && order.Tasks.Length != 0)
                {
                    if (order.Tasks.All(task => task.TaskType_Type == Enum.GetName(typeof(TaskType), TaskType.Delivery)))
                    {
                        taskSpecType = TaskSpecType.Delivery;
                    }
                    else if (order.Tasks.All(task => task.TaskType_Type == Enum.GetName(typeof(TaskType), TaskType.Pickup)))
                    {
                        taskSpecType = TaskSpecType.Pickup;
                    }
                    else
                    {
                        long firstTaskLocationEntityKey = order.Tasks.First().LocationEntityKey;
                        if (order.Tasks.All(task => task.LocationEntityKey == firstTaskLocationEntityKey))
                        {
                            taskSpecType = TaskSpecType.DeliveryAndPickup;
                        }
                        else
                        {
                            taskSpecType = TaskSpecType.Transfer;
                        }
                    }
                }
            }
            else
            {
                if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Delivery)))
                {
                    taskSpecType = TaskSpecType.Delivery;
                }
                else if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Pickup)))
                {
                    taskSpecType = TaskSpecType.Pickup;
                }
                else if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Transfer)))
                {
                    taskSpecType = TaskSpecType.Transfer;
                }
                else
                {
                    taskSpecType = TaskSpecType.DeliveryAndPickup;
                }
            }
            Task deliveryTask;
            Task pickupTask;
            switch (taskSpecType)
            {
                case TaskSpecType.Delivery:
                    orderSpec.TaskSpec = new DeliveryTaskSpec
                    {
                        AdditionalServiceTime = order.AdditionalServiceTime,
                        Quantities = order.PlannedDeliveryQuantities,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey

                    };
                    deliveryTask = order.Tasks.FirstOrDefault();
                    if (deliveryTask != null)
                    {
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).CoordinateOverride = deliveryTask.CoordinateOverride;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).OpenCloseOverrides = deliveryTask.OpenCloseOverrides;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).ServiceLocationEntityKey = deliveryTask.LocationEntityKey;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).ServiceWindowOverrides = deliveryTask.ServiceWindowOverrides;
                    }
                    break;
                case TaskSpecType.Pickup:
                    orderSpec.TaskSpec = new PickupTaskSpec
                    {
                        AdditionalServiceTime = order.AdditionalServiceTime,
                        Quantities = order.PlannedPickupQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,

                    };
                    pickupTask = order.Tasks.FirstOrDefault();
                    if (pickupTask != null)
                    {
                        ((PickupTaskSpec)orderSpec.TaskSpec).CoordinateOverride = pickupTask.CoordinateOverride;
                        ((PickupTaskSpec)orderSpec.TaskSpec).OpenCloseOverrides = pickupTask.OpenCloseOverrides;
                        ((PickupTaskSpec)orderSpec.TaskSpec).ServiceLocationEntityKey = pickupTask.LocationEntityKey;
                        ((PickupTaskSpec)orderSpec.TaskSpec).ServiceWindowOverrides = pickupTask.ServiceWindowOverrides;
                    }
                    break;
                case TaskSpecType.DeliveryAndPickup:
                    deliveryTask = order.Tasks[0];
                    pickupTask = order.Tasks[1];
                    orderSpec.TaskSpec = new DeliveryAndPickupTaskSpec
                    {
                        AdditionalServiceTime = deliveryTask.AdditionalServiceTime,
                        CoordinateOverride = deliveryTask.CoordinateOverride,
                        DeliveryQuantities = deliveryTask.PlannedQuantities,
                        OpenCloseOverrides = deliveryTask.OpenCloseOverrides,
                        PickupQuantities = pickupTask.PlannedQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey,
                        ServiceLocationEntityKey = deliveryTask.LocationEntityKey,
                        ServiceWindowOverrides = deliveryTask.ServiceWindowOverrides
                    };
                    break;
                case TaskSpecType.Transfer:
                    deliveryTask = order.Tasks[1];
                    pickupTask = order.Tasks[0];
                    orderSpec.TaskSpec = new TransferTaskSpec
                    {
                        DeliveryAdditionalServiceTime = TimeSpan.FromSeconds(order.AdditionalServiceTime.TotalSeconds / 2),
                        DeliveryCoordinateOverride = deliveryTask.CoordinateOverride,
                        DeliveryLocationEntityKey = deliveryTask.LocationEntityKey,
                        DeliveryOpenCloseOverrides = deliveryTask.OpenCloseOverrides,
                        DeliveryServiceWindowOverrides = deliveryTask.ServiceWindowOverrides,
                        PickupAdditionalServiceTime = TimeSpan.FromSeconds(order.AdditionalServiceTime.TotalSeconds / 2),
                        PickupCoordinateOverride = pickupTask.CoordinateOverride,
                        PickupLocationEntityKey = pickupTask.LocationEntityKey,
                        PickupOpenCloseOverrides = pickupTask.OpenCloseOverrides,
                        PickupServiceWindowOverrides = pickupTask.ServiceWindowOverrides,
                        Quantities = order.PlannedDeliveryQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey
                    };
                    break;
            }
            return orderSpec;
        }

        public static string ToString(Address address)
        {
            return string.Format("{0} | {1} | {2}",
                address.AddressLine1,
                address.AddressLine2,
                address.Locality != null ? ToString(address.Locality) : string.Empty);
        }

        public static string ToString(Coordinate coordinate)
        {
            return string.Format("{0} | {1}",
                coordinate.Latitude,
                coordinate.Longitude);
        }

        public static string ToString(CreateRouteArgs createRouteArgs)
        {
            return string.Format("{0} | {1}",
                createRouteArgs.RouteArgs != null ? ToString(createRouteArgs.RouteArgs) : string.Empty,
                createRouteArgs.Stops != null ? string.Join(" | ", createRouteArgs.Stops.Select(saveStopArgs => ToString(saveStopArgs))) : string.Empty);
        }

        public static string ToString(CreateRoutesResult.CreateRouteResult.CreateRouteErrors createRouteErrors)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                createRouteErrors.RouteIdentifier,
                createRouteErrors.StopIndex,
                createRouteErrors.OrderIdentifier,
                createRouteErrors.Failure != null ? ToString(createRouteErrors.Failure) : string.Empty);
        }

        public static string ToString(CreateRoutesResult.CreateRouteResult createRouteResult)
        {
            return string.Format("{0} | {1}",
                createRouteResult.RouteEntityKey,
                createRouteResult.Errors != null ? string.Join(" | ", createRouteResult.Errors.Select(error => ToString(error))) : string.Empty);
        }

        public static string ToString(CreateRoutesJobInfo createRoutesJobInfo)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                createRoutesJobInfo.EntityKey,
                createRoutesJobInfo.JobState_State,
                createRoutesJobInfo.StartTime.HasValue ? createRoutesJobInfo.StartTime.Value.ToString(DATETIME_FORMAT) : string.Empty,
                createRoutesJobInfo.FinishTime.HasValue ? createRoutesJobInfo.FinishTime.Value.ToString(DATETIME_FORMAT) : string.Empty,
                createRoutesJobInfo.PercentComplete,
                createRoutesJobInfo.Result != null ? ToString(createRoutesJobInfo.Result) : string.Empty,
                createRoutesJobInfo.JobFailureReason_FailureReason);
        }

        public static string ToString(CreateRoutesResult createRoutesResult)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                createRoutesResult.RoutesSubmitted,
                createRoutesResult.RoutesCreated,
                createRoutesResult.RoutesFailed,
                createRoutesResult.RouteResults != null ? string.Join(" | ", createRoutesResult.RouteResults.Select(routeResult => ToString(routeResult))) : string.Empty);
        }

        public static string ToString(CustomFormResponse customFormResponse)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10}",
                customFormResponse.RegionEntityKey,
                customFormResponse.EntityKey,
                customFormResponse.CustomFormEntityKey,
                customFormResponse.CustomFormIdentifier,
                customFormResponse.PerformedAt_PerformedAt,
                customFormResponse.EquipmentEntityKey,
                customFormResponse.RouteEntityKey,
                customFormResponse.StopEntityKey.HasValue ? customFormResponse.StopEntityKey.Value.ToString() : string.Empty,
                customFormResponse.OrderEntityKey.HasValue ? customFormResponse.OrderEntityKey.Value.ToString() : string.Empty,
                customFormResponse.LineItemEntityKey.HasValue ? customFormResponse.LineItemEntityKey.Value.ToString() : string.Empty,
                customFormResponse.Responses != null ? string.Join(" | ", customFormResponse.Responses.Select(response => ToString(response))) : string.Empty);
        }

        public static string ToString(DailyPass dailyPass)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                dailyPass.RegionEntityKey,
                dailyPass.SessionEntityKey,
                dailyPass.EntityKey,
                dailyPass.Identifier,
                dailyPass.Description,
                dailyPass.PassTemplateEntityKey);
        }

        public static string ToString(DailyRoutingSession dailyRoutingSession)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                dailyRoutingSession.RegionEntityKey,
                dailyRoutingSession.EntityKey,
                dailyRoutingSession.StartDate,
                dailyRoutingSession.Description,
                dailyRoutingSession.SessionMode_Mode,
                dailyRoutingSession.TimeUnit_TimeUnitType,
                dailyRoutingSession.NumberOfTimeUnits);
        }

        public static string ToString(DailyTimePeriod dailyTimePeriod)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                dailyTimePeriod.DayOfWeekFlags_DaysOfWeek,
                dailyTimePeriod.StartTime,
                dailyTimePeriod.EndTime,
                dailyTimePeriod.TimeSpan);
        }

        public static string ToString(DeliveryAndPickupTaskSpec deliveryAndPickupTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                deliveryAndPickupTaskSpec.ServiceLocationEntityKey,
                deliveryAndPickupTaskSpec.RequiredOriginEntityKey,
                deliveryAndPickupTaskSpec.RequiredDestinationEntityKey,
                deliveryAndPickupTaskSpec.DeliveryQuantities != null ? ToString(deliveryAndPickupTaskSpec.DeliveryQuantities) : string.Empty,
                deliveryAndPickupTaskSpec.PickupQuantities != null ? ToString(deliveryAndPickupTaskSpec.PickupQuantities) : string.Empty,
                deliveryAndPickupTaskSpec.OpenCloseOverrides != null ? string.Join(" | ", deliveryAndPickupTaskSpec.OpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                deliveryAndPickupTaskSpec.ServiceWindowOverrides != null ? string.Join(" | ", deliveryAndPickupTaskSpec.ServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(DeliveryTaskSpec deliveryTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4}",
                deliveryTaskSpec.ServiceLocationEntityKey,
                deliveryTaskSpec.RequiredOriginEntityKey,
                deliveryTaskSpec.Quantities != null ? ToString(deliveryTaskSpec.Quantities) : string.Empty,
                deliveryTaskSpec.OpenCloseOverrides != null ? string.Join(" | ", deliveryTaskSpec.OpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                deliveryTaskSpec.ServiceWindowOverrides != null ? string.Join(" | ", deliveryTaskSpec.ServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(Depot depot)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                depot.EntityKey,
                depot.Identifier,
                depot.BusinessUnitEntityKey,
                depot.CreatedInRegionEntityKey,
                depot.RegionEntityKeys != null ? string.Join(" | ", depot.RegionEntityKeys) : string.Empty,
                depot.VisibleInAllRegions,
                depot.Description,
                depot.Address != null ? ToString(depot.Address) : string.Empty);
        }

        public static string ToString(DomainInstance domainInstance)
        {
            return string.Format("{0} | {1}",
                domainInstance.EntityKey,
                domainInstance.Version);
        }

        public static string ToString(Equipment equipment)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11}",
                equipment.EntityKey,
                equipment.Identifier,
                equipment.BusinessUnitEntityKey,
                equipment.CreatedInRegionEntityKey,
                equipment.RegionEntityKeys != null ? string.Join(" | ", equipment.RegionEntityKeys) : string.Empty,
                equipment.VisibleInAllRegions,
                equipment.Description,
                equipment.EquipmentTypeEntityKey,
                equipment.IsActive,
                equipment.DepotEntityKey.HasValue ? equipment.DepotEntityKey.Value.ToString() : string.Empty,
                equipment.PreferredWorkerEntityKey.HasValue ? equipment.PreferredWorkerEntityKey.Value.ToString() : string.Empty,
                equipment.TelematicsDeviceEntityKey.HasValue ? equipment.TelematicsDeviceEntityKey.Value.ToString() : string.Empty);
        }

        public static string ToString(EquipmentType equipmentType)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                equipmentType.EntityKey,
                equipmentType.Identifier,
                equipmentType.BusinessUnitEntityKey,
                equipmentType.CreatedInRegionEntityKey,
                equipmentType.RegionEntityKeys != null ? string.Join(" | ", equipmentType.RegionEntityKeys) : string.Empty,
                equipmentType.VisibleInAllRegions,
                equipmentType.Description,
                equipmentType.EquipmentClassification_Classification,
                equipmentType.PowerUnit);
        }

        public static string ToString(FormControlResponse formControlResponse)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                formControlResponse.EntityKey,
                formControlResponse.FormControlEntityKey,
                formControlResponse.AnsweredTime.HasValue ? formControlResponse.AnsweredTime.Value.ToString(DATETIME_FORMAT) : string.Empty,
                formControlResponse.Value != null ? ToString(formControlResponse.Value) : string.Empty);
        }

        public static string ToString(FormControlResponseBinaryValue formControlResponseBinaryValue)
        {
            return string.Format("{0} | {1} | {2}",
                formControlResponseBinaryValue.BinaryDataType,
                formControlResponseBinaryValue.BinaryValue != null ? formControlResponseBinaryValue.BinaryValue.Length.ToString() : string.Empty,
                formControlResponseBinaryValue.TextValue);
        }

        public static string ToString(FormControlResponseBooleanValue formControlResponseBooleanValue)
        {
            return formControlResponseBooleanValue.BooleanValue.HasValue ? formControlResponseBooleanValue.BooleanValue.Value.ToString() : string.Empty;
        }

        public static string ToString(FormControlResponseDateTimeValue formControlResponseDateTimeValue)
        {
            return formControlResponseDateTimeValue.DateTimeValue.HasValue ? formControlResponseDateTimeValue.DateTimeValue.Value.ToString() : string.Empty;
        }

        public static string ToString(FormControlResponseInspectionValue formControlResponseInspectionValue)
        {
            return string.Format("{0} | {1}",
                formControlResponseInspectionValue.IsPositive,
                formControlResponseInspectionValue.TextValue);
        }

        public static string ToString(FormControlResponseNumericValue formControlResponseNumericValue)
        {
            return formControlResponseNumericValue.NumericValue.HasValue ? formControlResponseNumericValue.NumericValue.Value.ToString() : string.Empty;
        }

        public static string ToString(FormControlResponseTextValue formControlResponseTextValue)
        {
            return formControlResponseTextValue.TextValue;
        }

        public static string ToString(FormControlResponseValue formControlResponseValue)
        {
            if (formControlResponseValue is FormControlResponseBinaryValue)
            {
                return ToString((FormControlResponseBinaryValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseBooleanValue)
            {
                return ToString((FormControlResponseBooleanValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseDateTimeValue)
            {
                return ToString((FormControlResponseDateTimeValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseInspectionValue)
            {
                return ToString((FormControlResponseInspectionValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseNumericValue)
            {
                return ToString((FormControlResponseNumericValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseTextValue)
            {
                return ToString((FormControlResponseTextValue)formControlResponseValue);
            }
            else
            {
                return formControlResponseValue.ToString();
            }
        }

        public static string ToString(GeocodeCandidate geocodeCandidate)
        {
            return string.Format("{0} | {1} | {2}",
                geocodeCandidate.Coordinate != null ? ToString(geocodeCandidate.Coordinate) : string.Empty,
                geocodeCandidate.GeocodeAccuracy_Quality,
                geocodeCandidate.Score);
        }

        public static string ToString(LayoverDeparture layoverDeparture)
        {
            return layoverDeparture.ToString();
        }

        public static string ToString(LineItem lineItem)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                lineItem.EntityKey,
                lineItem.Identifier,
                lineItem.LineItemType_Type,
                lineItem.SKUEntityKey,
                lineItem.SKUIdentifier,
                lineItem.PlannedQuantities != null ? ToString(lineItem.PlannedQuantities) : string.Empty,
                lineItem.Quantities != null ? ToString(lineItem.Quantities) : string.Empty,
                lineItem.QuantitiesReasonCode != null ? ToString(lineItem.QuantitiesReasonCode) : string.Empty);
        }

        public static string ToString(Locality locality)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                locality.CountryISO3Abbr,
                locality.PostalCode,
                locality.AdminDivision1,
                locality.AdminDivision2,
                locality.AdminDivision3,
                locality.AdminDivision4,
                locality.AdminDivision5);
        }

        public static string ToString(ManipulationResult.ManipulationError manipulationError)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                manipulationError.RouteEntityKey,
                manipulationError.LocationEntityKey,
                manipulationError.StopEntityKey.HasValue ? manipulationError.StopEntityKey.Value.ToString() : string.Empty,
                manipulationError.OrderEntityKey.HasValue ? manipulationError.OrderEntityKey.Value.ToString() : string.Empty,
                manipulationError.Reason != null ? ToString(manipulationError.Reason) : string.Empty,
                manipulationError.MatchingPatternEntityKeys != null ? string.Join(" | ", manipulationError.MatchingPatternEntityKeys) : string.Empty);
        }

        public static string ToString(MobileDevice mobileDevice)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}",
                mobileDevice.EntityKey,
                mobileDevice.Identifier,
                mobileDevice.BusinessUnitEntityKey,
                mobileDevice.RegionEntityKeys != null ? string.Join(" | ", mobileDevice.RegionEntityKeys) : string.Empty,
                mobileDevice.Active,
                mobileDevice.DevicePhoneNumber,
                mobileDevice.EquipmentEntityKey.HasValue ? mobileDevice.EquipmentEntityKey.Value.ToString() : string.Empty,
                mobileDevice.MobileNetworkOperator_MobileNetworkOperator,
                mobileDevice.MobilePlatform_MobilePlatform,
                mobileDevice.VisibleInAllRegions);
        }

        public static string ToString(NamedPlace namedPlace)
        {
            return string.Format("{0} | {1}",
                namedPlace.PlaceName,
                namedPlace.PlaceAddress != null ? ToString(namedPlace.PlaceAddress) : string.Empty);
        }

        public static string ToString(Notification notification)
        {
            if (notification is RoutePhaseChangeNotification)
            {
                return ToString((RoutePhaseChangeNotification)notification);
            }
            else if (notification is RouteStateChangeNotification)
            {
                return ToString((RouteStateChangeNotification)notification);
            }
            else if (notification is StopSequenceNotification)
            {
                return ToString((StopSequenceNotification)notification);
            }
            else if (notification is StopStateChangeNotification)
            {
                return ToString((StopStateChangeNotification)notification);
            }
            else
            {
                return string.Format("{0} | {1} | {2} | {3} | {4}",
                    notification.EntityKey,
                    notification.NotificationType_NotificationType,
                    notification.RecipientEntityKey,
                    notification.RouteEntityKey.HasValue ? notification.RouteEntityKey.Value.ToString() : string.Empty,
                    notification.TelematicsDeviceEntityKey.HasValue ? notification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty);
            }
        }

        public static string ToString(Order order)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12} | {13} | {14} | {15} | {16}",
                order.RegionEntityKey,
                order.Identifier,
                order.OrderType_Type,
                order.OrderState_State,
                order.EntryMethod_EntryMethod,
                order.SessionEntityKey,
                order.BeginDate,
                order.EndDate,
                order.OrderClassEntityKey,
                order.ServiceTime,
                order.RequiredRouteOriginEntityKey,
                order.RequiredRouteDestinationEntityKey,
                order.PlannedDeliveryQuantities != null ? ToString(order.PlannedDeliveryQuantities) : string.Empty,
                order.PlannedPickupQuantities != null ? ToString(order.PlannedPickupQuantities) : string.Empty,
                order.DeliveryQuantities != null ? ToString(order.DeliveryQuantities) : string.Empty,
                order.PickupQuantities != null ? ToString(order.PickupQuantities) : string.Empty,
                order.LineItems != null ? string.Join(" | ", order.LineItems.Select(lineItem => ToString(lineItem))) : string.Empty);
        }

        public static string ToString(OrderSpec orderSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                orderSpec.RegionEntityKey,
                orderSpec.Identifier,
                orderSpec.SessionEntityKey,
                orderSpec.BeginDate,
                orderSpec.EndDate,
                orderSpec.OrderClassEntityKey,
                orderSpec.TaskSpec != null
                    ? (orderSpec.TaskSpec is DeliveryAndPickupTaskSpec
                        ? ToString((DeliveryAndPickupTaskSpec)orderSpec.TaskSpec)
                        : (orderSpec.TaskSpec is DeliveryTaskSpec
                            ? ToString((DeliveryTaskSpec)orderSpec.TaskSpec)
                            : (orderSpec.TaskSpec is PickupTaskSpec
                                ? ToString((PickupTaskSpec)orderSpec.TaskSpec)
                                : ToString((TransferTaskSpec)orderSpec.TaskSpec))))
                    : string.Empty,
                orderSpec.LineItems != null ? string.Join(" | ", orderSpec.LineItems.Select(lineItem => ToString(lineItem))) : string.Empty);
        }

        public static string ToString(PersonName personName)
        {
            return string.Format("{0} | {1} | {2}",
                personName.First,
                personName.Middle,
                personName.Last);
        }

        public static string ToString(PickupTaskSpec pickupTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4}",
                pickupTaskSpec.ServiceLocationEntityKey,
                pickupTaskSpec.RequiredDestinationEntityKey,
                pickupTaskSpec.Quantities != null ? ToString(pickupTaskSpec.Quantities) : string.Empty,
                pickupTaskSpec.OpenCloseOverrides != null ? string.Join(" | ", pickupTaskSpec.OpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                pickupTaskSpec.ServiceWindowOverrides != null ? string.Join(" | ", pickupTaskSpec.ServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(QualityPairedDateTime qualityPairedDateTime)
        {
            return string.Format("{0} | {1}",
                qualityPairedDateTime.Value,
                qualityPairedDateTime.DataQuality_Quality);
        }

        public static string ToString(Quantities quantities)
        {
            return string.Format("{0} | {1} | {2}",
                quantities.Size1,
                quantities.Size2,
                quantities.Size3);
        }

        public static string ToString(QuantityReasonCode quantityReasonCode)
        {
            return string.Format("{0} | {1} | {2}",
                quantityReasonCode.EntityKey,
                quantityReasonCode.Identifier,
                quantityReasonCode.QuantityType_QuantityTypes);
        }

        public static string ToString(Region region)
        {
            return string.Format("{0} | {1} | {2}",
                region.BusinessUnitEntityKey,
                region.EntityKey,
                region.Identifier);
        }

        public static string ToString(Route route)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12} | {13} | {14} | {15} | {16} | {17} | {18} | {19} | {20}",
                route.EntityKey,
                route.Identifier,
                route.Description,
                route.RegionEntityKey,
                route.RoutePhase_Phase,
                route.RouteState_State,
                route.RoutingSessionDate,
                route.RoutingSessionEntityKey,
                route.IsLoadedToDevice,
                route.LastStopIsDestination,
                route.OriginDepotEntityKey,
                route.OriginDepotIdentifier,
                route.DestinationDepotEntityKey,
                route.DestinationDepotIdentifier,
                route.ArrivalTime != null ? ToString(route.ArrivalTime) : string.Empty,
                route.CompleteTime != null ? ToString(route.CompleteTime) : string.Empty,
                route.DepartureTime != null ? ToString(route.DepartureTime) : string.Empty,
                route.StartTime != null ? ToString(route.StartTime) : string.Empty,
                route.Equipment != null ? string.Join(" | ", route.Equipment.Select(equipment => ToString(equipment))) : string.Empty,
                route.Workers != null ? string.Join(" | ", route.Workers.Select(worker => ToString(worker))) : string.Empty,
                route.Stops != null ? string.Join(" | ", route.Stops.Select(stop => ToString(stop))) : string.Empty);
        }

        public static string ToString(RouteEquipment routeEquipment)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                routeEquipment.EntityKey,
                routeEquipment.EquipmentEntityKey,
                routeEquipment.EquipmentIdentifier,
                routeEquipment.EquipmentDescription);
        }

        public static string ToString(RouteEquipmentBase routeEquipmentBase)
        {
            if (routeEquipmentBase is RouteEquipment)
            {
                return ToString((RouteEquipment)routeEquipmentBase);
            }
            else if (routeEquipmentBase is RouteEquipmentType)
            {
                return ToString((RouteEquipmentType)routeEquipmentBase);
            }
            else
            {
                return routeEquipmentBase.EntityKey.ToString();
            }
        }

        public static string ToString(RouteEquipmentType routeEquipmentType)
        {
            return string.Format("{0} | {1}",
                routeEquipmentType.EntityKey,
                routeEquipmentType.EquipmentTypeEntityKey);
        }

        public static string ToString(RoutePhaseChangeNotification routePhaseChangeNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                routePhaseChangeNotification.EntityKey,
                routePhaseChangeNotification.NotificationType_NotificationType,
                routePhaseChangeNotification.RecipientEntityKey,
                routePhaseChangeNotification.RouteEntityKey.HasValue ? routePhaseChangeNotification.RouteEntityKey.Value.ToString() : string.Empty,
                routePhaseChangeNotification.TelematicsDeviceEntityKey.HasValue ? routePhaseChangeNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                routePhaseChangeNotification.RoutePhase_Phase);
        }

        public static string ToString(RouteStateChangeNotification routeStateChangeNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                routeStateChangeNotification.EntityKey,
                routeStateChangeNotification.NotificationType_NotificationType,
                routeStateChangeNotification.RecipientEntityKey,
                routeStateChangeNotification.RouteEntityKey.HasValue ? routeStateChangeNotification.RouteEntityKey.Value.ToString() : string.Empty,
                routeStateChangeNotification.TelematicsDeviceEntityKey.HasValue ? routeStateChangeNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                routeStateChangeNotification.RouteState_State);
        }

        public static string ToString(RouteWorker routeWorker)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4}",
                routeWorker.EntityKey,
                routeWorker.Index,
                routeWorker.WorkerEntityKey,
                routeWorker.WorkerIdentifier,
                routeWorker.WorkerName != null ? ToString(routeWorker.WorkerName) : string.Empty);
        }

        public static string ToString(SaveAssignedOrderArgs saveAssignedOrderArgs)
        {
            return string.Format("{0} | {1} | {2}",
                saveAssignedOrderArgs.Identifier,
                saveAssignedOrderArgs.OrderClassEntityKey,
                saveAssignedOrderArgs.DeliveryQuantities != null ? ToString(saveAssignedOrderArgs.DeliveryQuantities) : string.Empty,
                saveAssignedOrderArgs.PickupQuantities != null ? ToString(saveAssignedOrderArgs.PickupQuantities) : string.Empty,
                saveAssignedOrderArgs.LineItems != null ? string.Join(" | ", saveAssignedOrderArgs.LineItems.Select(lineItem => ToString(lineItem))) : string.Empty);
        }

        public static string ToString(SaveErrorInfo saveErrorInfo)
        {
            return string.Format("{0} | {1} | {2}",
                saveErrorInfo.Code != null ? saveErrorInfo.Code.ErrorCode_Status : string.Empty,
                saveErrorInfo.Detail,
                saveErrorInfo.ValidationFailures != null ? string.Join(" | ", saveErrorInfo.ValidationFailures.Select(validationFailure => ToString(validationFailure))) : string.Empty);
        }

        public static string ToString(SaveLayoverStopArgs saveLayoverStopArgs)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                saveLayoverStopArgs.EntityKey,
                saveLayoverStopArgs.Coordinate != null ? ToString(saveLayoverStopArgs.Coordinate) : string.Empty,
                saveLayoverStopArgs.Departure != null ? ToString(saveLayoverStopArgs.Departure) : string.Empty,
                saveLayoverStopArgs.LayoverLocationEntityKey.HasValue ? saveLayoverStopArgs.LayoverLocationEntityKey.Value.ToString() : string.Empty,
                saveLayoverStopArgs.Paid,
                saveLayoverStopArgs.PlacementType_PlacementType,
                Enum.GetName(typeof(WorldTimeZone), saveLayoverStopArgs.TimeZone));
        }

        public static string ToString(SaveMidRouteDepotStopArgs saveMidRouteDepotStopArgs)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                saveMidRouteDepotStopArgs.EntityKey,
                saveMidRouteDepotStopArgs.DepotEntityKey,
                saveMidRouteDepotStopArgs.LoadAction.HasValue ? Enum.GetName(typeof(LoadAction), saveMidRouteDepotStopArgs.LoadAction.Value) : string.Empty,
                saveMidRouteDepotStopArgs.ReloadTimeOverride.HasValue ? saveMidRouteDepotStopArgs.ReloadTimeOverride.Value.ToString(TIMESPAN_FORMAT) : string.Empty);
        }

        public static string ToString(SaveNonServiceableStopArgs saveNonServiceableStopArgs)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                saveNonServiceableStopArgs.EntityKey,
                saveNonServiceableStopArgs.Coordinate != null ? ToString(saveNonServiceableStopArgs.Coordinate) : string.Empty,
                saveNonServiceableStopArgs.Duration != null ? saveNonServiceableStopArgs.Duration.ToString(TIMESPAN_FORMAT) : string.Empty,
                saveNonServiceableStopArgs.Paid,
                Enum.GetName(typeof(PlacementType), saveNonServiceableStopArgs.PlacementType),
                Enum.GetName(typeof(WorldTimeZone), saveNonServiceableStopArgs.TimeZone));
        }

        public static string ToString(SaveRouteArgs saveRouteArgs)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                saveRouteArgs.Identifier,
                saveRouteArgs.Description,
                Enum.GetName(typeof(RoutePhase), saveRouteArgs.Phase),
                saveRouteArgs.OriginDepotEntityKey,
                saveRouteArgs.DestinationEntityKey.HasValue ? saveRouteArgs.DestinationEntityKey.Value.ToString() : string.Empty,
                saveRouteArgs.LastStopIsDestination,
                saveRouteArgs.StartTime != null ? saveRouteArgs.StartTime.ToString(DATETIME_FORMAT) : string.Empty,
                saveRouteArgs.Workers != null ? string.Join(" | ", saveRouteArgs.Workers.Select(worker => ToString(worker))) : string.Empty,
                saveRouteArgs.Equipment != null ? string.Join(" | ", saveRouteArgs.Equipment.Select(equipment => ToString(equipment))) : string.Empty);
        }

        public static string ToString(SaveServiceableStopArgs saveServiceableStopArgs)
        {
            return string.Format("{0} | {1} | {2}",
                saveServiceableStopArgs.ServiceLocationEntityKey,
                saveServiceableStopArgs.CoordinateOverride != null ? ToString(saveServiceableStopArgs.CoordinateOverride) : string.Empty,
                saveServiceableStopArgs.Orders != null ? string.Join(" | ", saveServiceableStopArgs.Orders.Select(order => ToString(order))) : string.Empty);
        }

        public static string ToString(SaveStopArgs saveStopArgs)
        {
            if (saveStopArgs is SaveLayoverStopArgs)
            {
                return ToString((SaveLayoverStopArgs)saveStopArgs);
            }
            else if (saveStopArgs is SaveMidRouteDepotStopArgs)
            {
                return ToString((SaveMidRouteDepotStopArgs)saveStopArgs);
            }
            else if (saveStopArgs is SaveNonServiceableStopArgs)
            {
                return ToString((SaveNonServiceableStopArgs)saveStopArgs);
            }
            if (saveStopArgs is SaveServiceableStopArgs)
            {
                return ToString((SaveServiceableStopArgs)saveStopArgs);
            }
            else
            {
                return saveStopArgs.ToString();
            }
        }

        public static string ToString(ServiceableStop serviceableStop)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12}",
                serviceableStop.EntityKey,
                serviceableStop.ServiceLocationEntityKey,
                serviceableStop.ServiceLocationIdentifier,
                serviceableStop.StopState_State,
                serviceableStop.IsCancelled,
                serviceableStop.IsUnserviceable,
                serviceableStop.RouteIdentifier,
                serviceableStop.SequenceNumber,
                serviceableStop.Index,
                serviceableStop.ArrivalTime != null ? ToString(serviceableStop.ArrivalTime) : string.Empty,
                serviceableStop.DepartureTime != null ? ToString(serviceableStop.DepartureTime) : string.Empty,
                serviceableStop.HasSignature,
                serviceableStop.Actions != null ? string.Join(" | ", serviceableStop.Actions.Select(action => ToString(action))) : string.Empty);
        }

        public static string ToString(ServiceLocation serviceLocation)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                serviceLocation.Identifier,
                serviceLocation.EntityKey,
                serviceLocation.BusinessUnitEntityKey,
                serviceLocation.CreatedInRegionEntityKey,
                serviceLocation.Description
                );
        }

        public static string ToString(ServiceTimeDetail serviceTimeDetail)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}",
                serviceTimeDetail.EntityKey,
                serviceTimeDetail.DayOfWeekFlags_Days,
                serviceTimeDetail.FixedNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitVariableNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.BulkFixedNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitBulkVariableNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.FixedHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitVariableHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.BulkFixedHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitBulkVariableHelper.ToString(TIMESPAN_FORMAT));
        }

        public static string ToString(SKU sku)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                sku.EntityKey,
                sku.Identifier,
                sku.Description,
                sku.PackageTypeEntityKey,
                sku.PackageTypeIdentifier,
                sku.PackageTypeDescription,
                sku.ServiceTimeDetails != null ? string.Join(" | ", sku.ServiceTimeDetails.Select(serviceTimeDetail => ToString(serviceTimeDetail))) : string.Empty);
        }

        public static string ToString(Stop stop)
        {
            if (stop is ServiceableStop)
            {
                return ToString((ServiceableStop)stop);
            }
            else
            {
                return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                    stop.EntityKey,
                    stop.StopState_State,
                    stop.IsCancelled,
                    stop.Index,
                    stop.ArrivalTime != null ? ToString(stop.ArrivalTime) : string.Empty,
                    stop.DepartureTime != null ? ToString(stop.DepartureTime) : string.Empty);
            }
        }

        public static string ToString(StopAction stopAction)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                stopAction.EntityKey,
                stopAction.OrderEntityKey,
                stopAction.OrderIdentifier,
                stopAction.OrderState_OrderState,
                stopAction.OrderType_OrderType,
                stopAction.StopActionType_Type,
                stopAction.StopActionLineItemQuantities != null ? string.Join(" | ", stopAction.StopActionLineItemQuantities.Select(stopActionLineItemQuantities => ToString(stopActionLineItemQuantities))) : string.Empty);
        }

        public static string ToString(StopActionLineItemQuantities stopActionLineItemQuantities)
        {
            return string.Format("{0} | {1}",
                stopActionLineItemQuantities.EntityKey,
                stopActionLineItemQuantities.LineItem != null ? ToString(stopActionLineItemQuantities.LineItem) : string.Empty);
        }

        public static string ToString(Stop.Identity stopIdentity)
        {
            return string.Format("{0} | {1}",
                stopIdentity.StopEntityKey.HasValue ? stopIdentity.StopEntityKey.Value.ToString() : string.Empty,
                stopIdentity.DeviceStopId.HasValue ? stopIdentity.DeviceStopId.Value.ToString() : string.Empty);
        }

        public static string ToString(StopSequenceNotification stopSequenceNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                stopSequenceNotification.EntityKey,
                stopSequenceNotification.NotificationType_NotificationType,
                stopSequenceNotification.RecipientEntityKey,
                stopSequenceNotification.RouteEntityKey.HasValue ? stopSequenceNotification.RouteEntityKey.Value.ToString() : string.Empty,
                stopSequenceNotification.TelematicsDeviceEntityKey.HasValue ? stopSequenceNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                stopSequenceNotification.Stops != null ? string.Join(" | ", stopSequenceNotification.Stops.Select(stop => ToString(stop))) : string.Empty);
        }

        public static string ToString(StopStateChangeNotification stopStateChangeNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                stopStateChangeNotification.EntityKey,
                stopStateChangeNotification.NotificationType_NotificationType,
                stopStateChangeNotification.RecipientEntityKey,
                stopStateChangeNotification.RouteEntityKey.HasValue ? stopStateChangeNotification.RouteEntityKey.Value.ToString() : string.Empty,
                stopStateChangeNotification.TelematicsDeviceEntityKey.HasValue ? stopStateChangeNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                stopStateChangeNotification.StopEntityKey,
                stopStateChangeNotification.StopState_State,
                stopStateChangeNotification.IsCancelled);
        }

        public static string ToString(TaskOpenCloseOverrideDetail taskOpenCloseOverrideDetail)
        {
            return string.Format("{0} | {1}",
                taskOpenCloseOverrideDetail.EntityKey,
                taskOpenCloseOverrideDetail.DailyTimePeriod != null ? ToString(taskOpenCloseOverrideDetail.DailyTimePeriod) : string.Empty);
        }

        public static string ToString(TaskServiceWindowOverrideDetail taskServiceWindowOverrideDetail)
        {
            return string.Format("{0} | {1} | {2}",
                taskServiceWindowOverrideDetail.EntityKey,
                taskServiceWindowOverrideDetail.DailyTimePeriod != null ? ToString(taskServiceWindowOverrideDetail.DailyTimePeriod) : string.Empty,
                taskServiceWindowOverrideDetail.RequiresMultipleWorkers);
        }

        public static string ToString(TelematicsDevice telematicsDevice)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                telematicsDevice.EntityKey,
                telematicsDevice.Identifier,
                telematicsDevice.BusinessUnitEntityKey,
                telematicsDevice.RegionEntityKeys != null ? string.Join(" | ", telematicsDevice.RegionEntityKeys) : string.Empty,
                telematicsDevice.Active,
                telematicsDevice.DevicePhoneNumber,
                telematicsDevice.EquipmentEntityKey.HasValue ? telematicsDevice.EquipmentEntityKey.Value.ToString() : string.Empty,
                telematicsDevice.TelematicsProviderType_ProviderType,
                telematicsDevice.VisibleInAllRegions);
        }

        public static string ToString(TransferErrorCode transferErrorCode)
        {
            return transferErrorCode.ErrorCode_Status;
        }

        public static string ToString(TransferTaskSpec transferTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                transferTaskSpec.PickupLocationEntityKey,
                transferTaskSpec.DeliveryLocationEntityKey,
                transferTaskSpec.RequiredOriginEntityKey,
                transferTaskSpec.RequiredDestinationEntityKey,
                transferTaskSpec.Quantities != null ? ToString(transferTaskSpec.Quantities) : string.Empty,
                transferTaskSpec.PickupOpenCloseOverrides != null ? string.Join(" | ", transferTaskSpec.PickupOpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                transferTaskSpec.DeliveryOpenCloseOverrides != null ? string.Join(" | ", transferTaskSpec.DeliveryOpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                transferTaskSpec.PickupServiceWindowOverrides != null ? string.Join(" | ", transferTaskSpec.PickupServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty,
                transferTaskSpec.DeliveryServiceWindowOverrides != null ? string.Join(" | ", transferTaskSpec.DeliveryServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(UnassignedOrderGroup unassignedOrderGroup)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                unassignedOrderGroup.RegionEntityKey,
                unassignedOrderGroup.SessionEntityKey,
                unassignedOrderGroup.SessionDate,
                unassignedOrderGroup.SessionDescription,
                unassignedOrderGroup.SessionMode_SessionMode,
                unassignedOrderGroup.EntityKey,
                unassignedOrderGroup.LocationEntityKey,
                unassignedOrderGroup.LocationIdentifier,
                unassignedOrderGroup.OrderEntityKeys != null ? string.Join(" | ", unassignedOrderGroup.OrderEntityKeys) : string.Empty,
                unassignedOrderGroup.OrderIdentifiers != null ? string.Join(" | ", unassignedOrderGroup.OrderIdentifiers) : string.Empty,
                unassignedOrderGroup.BeginDate,
                unassignedOrderGroup.EndDate);
        }

        public static string ToString(ValidationFailure validationFailure)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                validationFailure.FailureType_Type,
                validationFailure.Property,
                validationFailure.Value,
                validationFailure.Minimum,
                validationFailure.Maximum,
                validationFailure.Pattern);
        }

        public static string ToString(Worker worker)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}",
                worker.EntityKey,
                worker.Identifier,
                worker.BusinessUnitEntityKey,
                worker.CreatedInRegionEntityKey,
                worker.RegionEntityKeys != null ? string.Join(" | ", worker.RegionEntityKeys) : string.Empty,
                worker.VisibleInAllRegions,
                worker.Name != null ? ToString(worker.Name) : string.Empty,
                worker.WorkerTypeEntityKey,
                worker.DepotEntityKey.HasValue ? worker.DepotEntityKey.Value.ToString() : string.Empty,
                worker.EquipmentEntityKey.HasValue ? worker.EquipmentEntityKey.Value.ToString() : string.Empty);
        }

        public static string ToString(WorkerType workerType)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                workerType.EntityKey,
                workerType.Identifier,
                workerType.BusinessUnitEntityKey,
                workerType.CreatedInRegionEntityKey,
                workerType.RegionEntityKeys != null ? string.Join(" | ", workerType.RegionEntityKeys) : string.Empty,
                workerType.VisibleInAllRegions);
        }

        #endregion

        #region Private Methods
        private string GetEnumDescription(Enum value)
        {
            // Get the Description attribute value for the enum value
            FieldInfo fi = value.GetType().GetField(value.ToString());
            DescriptionAttribute[] attributes =
                (DescriptionAttribute[])fi.GetCustomAttributes(
                    typeof(DescriptionAttribute), false);

            if (attributes.Length > 0)
            {
                return attributes[0].Description;
            }
            else
            {
                return value.ToString();
            }
        }
        #endregion
        #region Public Methods

        public ApexConsumer(
            Region region,
            log4net.ILog logger)
        {
            _BusinessUnitEntityKey = region.BusinessUnitEntityKey;
            _Region = region;
            _RegionContext = new SingleRegionContext
            {
                BusinessUnitEntityKey = region.BusinessUnitEntityKey,
                RegionEntityKey = region.EntityKey
            };
            _Logger = logger;
            _QueryServiceClient = new QueryServiceClient("BasicHttpBinding_IQueryService", MainService.QueryServiceUrl);
            _MappingServiceClient = new MappingServiceClient("BasicHttpBinding_IMappingService", MainService.RegionUrlSets[region.EntityKey].MappingService);
            _RoutingServiceClient = new RoutingServiceClient("BasicHttpBinding_IRoutingService", MainService.RegionUrlSets[region.EntityKey].RoutingService);
        }

        public ApexConsumer(
            long businessUnitEntityKey,
            log4net.ILog logger)
        {
            _BusinessUnitEntityKey = businessUnitEntityKey;
            _RegionContext = new MultipleRegionContext
            {
                BusinessUnitEntityKey = businessUnitEntityKey,
                Mode = MultipleRegionMode.All
            };
            _Logger = logger;
            _QueryServiceClient = new QueryServiceClient("BasicHttpBinding_IQueryService", MainService.QueryServiceUrl);
            _RoutingServiceClient = new RoutingServiceClient("BasicHttpBinding_IRoutingService", MainService.DefaultRoutingServiceUrl);
        }

        public SaveResult[] DeleteNotifications(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            Notification[] notifications)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    null,
                    notifications.Select(notification => new Notification
                    {
                        Action = ActionType.Delete,
                        EntityKey = notification.EntityKey,
                        Version = notification.Version
                    }).ToArray(),
                    new SaveOptions
                    {
                        InclusionMode = PropertyInclusionMode.All
                    });
                if (saveResults == null)
                {
                    _Logger.Error("DeleteNotifications | " + string.Join(" | ", notifications.Select(notification => ToString(notification))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("DeleteNotifications | " + ToString(notifications[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("DeleteNotifications | " + string.Join(" | ", notifications.Select(notification => ToString(notification))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("DeleteNotifications | " + string.Join(" | ", notifications.Select(notification => ToString(notification))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        public GeocodeResult[] Geocode(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            out bool timeout,
            Address[] addresses)
        {
            GeocodeResult[] geocodeResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            timeout = false;
            try
            {
                geocodeResults = _MappingServiceClient.Geocode(
                    MainService.SessionHeader,
                    _RegionContext,
                    new GeocodeCriteria
                    {
                        NamedPlaces = addresses.Select(address => new NamedPlace { PlaceAddress = address }).ToArray()

                    },
                    new GeocodeOptions
                    {
                        NetworkArcCandidatePropertyInclusionMode = PropertyInclusionMode.All,
                        NetworkPOICandidatePropertyInclusionMode = PropertyInclusionMode.All,
                        NetworkPointAddressCandidatePropertyInclusionMode = PropertyInclusionMode.All

                    });
                if (geocodeResults == null)
                {
                    _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < geocodeResults.Length; i++)
                    {
                        if (geocodeResults[i] == null || geocodeResults[i].Results == null || geocodeResults[i].Results.Length == 0)
                        {
                            _Logger.Error("Geocode | " + ToString(addresses[i]) + " | Failed with a null result.");
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))), ex);
                errorLevel = ErrorLevel.Transient;
                timeout = ex is TimeoutException || ex is CommunicationException;
            }
            return geocodeResults;
        }


        public GeocodeCandidate GeocodeRNA(
          out bool errorCaught,
          out string fatalErrorMessage,
          out bool timeout,
          Address[] addresses)
        {
            List<GeocodeCandidate> listCandidate = new List<GeocodeCandidate>();
            GeocodeResult[] geocodeResults = null;
            errorCaught = false;
            fatalErrorMessage = string.Empty;
            timeout = false;
            try
            {
                geocodeResults = _MappingServiceClient.Geocode(
                    MainService.SessionHeader,
                    _RegionContext,
                    new GeocodeCriteria
                    {
                        NamedPlaces = addresses.Select(address => new NamedPlace { PlaceAddress = address }).ToArray()

                    },
                    new GeocodeOptions
                    {
                        NetworkArcCandidatePropertyInclusionMode = PropertyInclusionMode.All,
                        NetworkPOICandidatePropertyInclusionMode = PropertyInclusionMode.All,
                        NetworkPointAddressCandidatePropertyInclusionMode = PropertyInclusionMode.All

                    });
                if (geocodeResults == null)
                {
                    _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))) + " | Failed with a null result.");
                    errorCaught = true ;
                }
                else
                {

                    for (int i = 0; i < geocodeResults.Length; i++)
                    {
                        if (geocodeResults[i] == null || geocodeResults[i].Results == null || geocodeResults[i].Results.Length == 0)
                        {
                            _Logger.Error("Geocode | " + ToString(addresses[i]) + " | Failed with a null result.");
                            errorCaught = false;
                        }
                    }
                    List<GeocodeResult> result = geocodeResults.Where(x => x.Results.All(y => y != null)).ToList();
                    List<GeocodeCandidate[]> temp = result.Select(x => x.Results).ToList();

                    foreach (GeocodeCandidate[] candidate in temp)
                    {
                        foreach(GeocodeCandidate c in candidate)
                        {
                            listCandidate.Add(c);
                        }

                    }
                    
                    return listCandidate.MinBy(x => (int)Enum.Parse(typeof(GeocodeAccuracy), x.GeocodeAccuracy_Quality)).First();

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))), ex);
                errorCaught = true;
                timeout = ex is TimeoutException || ex is CommunicationException;
            }
            return listCandidate.MinBy(x => (int)Enum.Parse(typeof(GeocodeAccuracy), x.GeocodeAccuracy_Quality)).First();
        }

        public DailyRoutingSession[] RetrieveDailyRoutingSessions(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            DateTime[] startDates)
        {
            DailyRoutingSession[] dailyRoutingSessions = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "Description" },
                                    Right = new ValueExpression { Value = DEFAULT_IDENTIFIER }
                                },
                                new InExpression
                                {
                                    Left = new PropertyExpression { Name = "StartDate" },
                                    Right = new ValueExpression { Value = startDates.Select(startDate => startDate.ToString(DATE_FORMAT)).ToArray() }
                                }
                            }
                        },
                        //TODO
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new DailyRoutingSessionPropertyOptions
                        {
                            StartDate = true
                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.DailyRoutingSession)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    dailyRoutingSessions = retrievalResults.Items.Cast<DailyRoutingSession>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDates), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return dailyRoutingSessions;
        }

        public Notification[] RetrieveNotifications(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            long recipientEntityKey)
        {
            Notification[] notifications = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new EqualToExpression
                        {
                            Left = new PropertyExpression { Name = "RecipientEntityKey" },
                            Right = new ValueExpression { Value = recipientEntityKey }
                        },
                        Paged = true,
                        PageIndex = 0,
                        PageSize = BATCH_SIZE,
                        PropertyInclusionMode = PropertyInclusionMode.All,
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Notification)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveNotifications | " + recipientEntityKey + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    notifications = retrievalResults.Items.Cast<Notification>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveNotifications | " + recipientEntityKey + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveNotifications | " + recipientEntityKey, ex);
                errorLevel = ErrorLevel.Transient;
            }
            return notifications;
        }

        public void RetrieveRNARoutesAndOrdersWriteThemToStagingTable(out bool errorRWRoutes, out string errorRWRoutesMessage)
        {
            errorRWRoutes = false;
            errorRWRoutesMessage = string.Empty;
            ErrorLevel errorLevel = ErrorLevel.None;
            string fatalErrorMessage = string.Empty;
            List<Route> modifiedRoutes = new List<Route>();
            List<UnassignedOrderGroup> unassignedOrders = new List<UnassignedOrderGroup>();
            DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);

            try
            {
                _Logger.Debug("Start Retrieving Unassigned Orders");
                unassignedOrders = RetrieveRNAUnassignedOrderGroups(out errorLevel, out fatalErrorMessage, RegionProcessor.lastSuccessfulRunTime);
                if (errorLevel == ErrorLevel.None)
                {
                    _Logger.Debug("Completed Retrieving Unassigned Orders");


                    if (unassignedOrders.Count != 0 && unassignedOrders != null)
                    {
                        foreach (UnassignedOrderGroup orderGroup in unassignedOrders)
                        {
                            foreach (String orderIdentifier in orderGroup.OrderIdentifiers)
                            {
                                DBAccessor.InsertStagedUnassignedOrders(_Region.Identifier, orderIdentifier, DateTime.Now.ToString(), "", "NEW", out errorRWRoutesMessage, out errorRWRoutes);

                            }
                        }
                    }
                }
                else if (errorLevel == ErrorLevel.Fatal)
                {
                    _Logger.Error(fatalErrorMessage);
                }
                else if (errorLevel == ErrorLevel.Transient)
                {
                    _Logger.Error(fatalErrorMessage);
                }

                //Retrieve Routes Modified by RNA
                _Logger.Debug("Start Retrieving Modified Routes");

                modifiedRoutes = RetrieveModifiedRNARoute(out errorLevel, out fatalErrorMessage, RegionProcessor.lastSuccessfulRunTime);
                if (errorLevel == ErrorLevel.None)
                {
                    if (modifiedRoutes != null)
                    {
                        _Logger.DebugFormat("Completed Retrieving {0} Modified Routes", modifiedRoutes.Count);

                        //Write Routes and Unassigned Orders to Routing Table
                        foreach (Route route in modifiedRoutes)
                        {
                            if (route.Stops.Count() != 0 && route.RoutePhase_Phase != Enum.GetName(typeof(RoutePhase), RoutePhase.Archive))
                            {
                                foreach (Stop stop in route.Stops)
                                {

                                    if (stop is ServiceableStop)
                                    {
                                        var tempStop = (ServiceableStop)stop;

                                        foreach (StopAction action in tempStop.Actions)
                                        {
                                            if (unassignedOrders == null)
                                            {
                                                DBAccessor.InsertStagedRoute(action.OrderIdentifier, _Region.Identifier, route.Identifier, route.StartTime.Value.ToString(), route.Description, tempStop.SequenceNumber.ToString(), DateTime.Now.ToString(), "", "NEW");
                                            }
                                            else if (!unassignedOrders.Exists(x => x.OrderEntityKeys.Contains(action.EntityKey)))
                                            {
                                                DBAccessor.InsertStagedRoute(action.OrderIdentifier, _Region.Identifier, route.Identifier, route.StartTime.Value.ToString(), route.Description, tempStop.SequenceNumber.ToString(), DateTime.Now.ToString(), "", "NEW");
                                            }



                                        }
                                    }
                                }
                            }
                            else
                            {
                                _Logger.DebugFormat("Route {0} contains no stops. No Information was Added to the route table information ", route.Identifier);
                            }
                        }
                    } else
                    {
                        _Logger.DebugFormat("No Modiefied Routes Found");
                    }
                }
                else if (errorLevel == ErrorLevel.Fatal)
                {
                    _Logger.Error(fatalErrorMessage);
                }
                else if (errorLevel == ErrorLevel.Transient)
                {
                    _Logger.Error(fatalErrorMessage);
                }

            }
            catch (Exception ex)
            {
                _Logger.Error("Error Retrieving Routes from RNA and Wrting Routes to Staging Table | " + ex.Message);
                errorRWRoutes = true;
                errorRWRoutesMessage = "Error Retrieving Routes from RNA and Wrting Routes to Staging Table | " + ex.Message;
            }

        }

        public List<Route> RetrieveModifiedRNARoute(out ErrorLevel errorLevel, out string fatalErrorMessage, DateTime lastCycleTime)
        {
            List<Route> routes = null;


            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new GreaterThanExpression
                        {
                            Left = new PropertyExpression { Name = "ModifiedTime" },
                            Right = new ValueExpression { Value = lastCycleTime }
                        },


                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new RoutePropertyOptions
                        {
                            Identifier = true,
                            RegionEntityKey = true,
                            Description = true,
                            StartTime = true,
                            Stops = true,
                            StopsOptions = new StopPropertyOptions
                            {
                                Actions = true,
                                SequenceNumber = true,
                                ActionsOptions = new StopActionPropertyOptions
                                {
                                    OrderIdentifier = true

                                },

                            }
                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Route)
                    });
                if (retrievalResults.Items == null)
                {
                    fatalErrorMessage = "Failed with a null result.";
                    _Logger.Error("Retrieve Routes | Modified after/before " + lastCycleTime.ToLongDateString() + " | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Fatal;
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "No Routes Found In RNA";
                    _Logger.Error("Retrieve Routes | Modified after/before" + lastCycleTime.ToLongDateString() + " Failed | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.None;
                    routes = retrievalResults.Items.Cast<Route>().ToList();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Routes | Modified after/before " + lastCycleTime.ToLongDateString() + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Routes | Modified after/before" + lastCycleTime.ToLongDateString(), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return routes;
        }

        public List<UnassignedOrderGroup> RetrieveRNAUnassignedOrderGroups(out ErrorLevel errorLevel, out string fatalErrorMessage, DateTime lastCycleTime)
        {
            List<UnassignedOrderGroup> unassignedOrders = null;

            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new GreaterThanExpression
                                {
                                    Left = new PropertyExpression { Name = "ModifiedTime" },
                                    Right = new ValueExpression { Value = lastCycleTime },
                                },


                            }
                        },


                        PropertyInclusionMode = PropertyInclusionMode.All,

                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.UnassignedOrderGroup)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Unassigned Order | Modified after/before " + lastCycleTime.ToLongDateString() + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Fatal;
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "Unassigned Orders do not exist.";
                    _Logger.Error("Retrieve Unassigned Order  | Modified after/before" + lastCycleTime.ToLongDateString() + " | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Transient;
                    unassignedOrders = null;
                }
                else
                {
                    errorLevel = ErrorLevel.None;
                    _Logger.DebugFormat("Retrieved {0} Unassigned Order Successful | Modified after/before" + lastCycleTime.ToLongDateString(), retrievalResults.Items.Count());
                    unassignedOrders = retrievalResults.Items.Cast<UnassignedOrderGroup>().ToList();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Unassigned Order  | Modified after/before " + lastCycleTime.ToLongDateString() + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Unassigned Order  | Modified after/before" + lastCycleTime.ToLongDateString(), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return unassignedOrders;
        }

        public List<Order> RetrieveCSVOrdersFromRNA(out ErrorLevel errorLevel, out string fatalErrorMessage, string[] identifiers)
        {
            List<Order> orders = null;


            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifiers }
                        },


                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new OrderPropertyOptions
                        {
                            Identifier = true,
                            RegionEntityKey = true,
                            Tasks = true,
                            BeginDate = true,
                            OrderClassEntityKey = true,
                            SessionEntityKey = true,
                            ManagedByUserEntityKey = true,
                            PickupQuantities = true,
                            RequiredRouteOriginEntityKey = true,
                            RequiredRouteDestinationEntityKey = true,




                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Order)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Orders | Matching Dummy Pick Up Orders " + identifiers.ToString() + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "Orders doe not exist.";
                    _Logger.Error("Retrieve Orders |  Matching Dummy Pick Up Orders " + identifiers.ToString() + " | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Fatal;
                }
                else
                {
                    orders = retrievalResults.Items.Cast<Order>().ToList();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Orders |  Matching Dummy Pick Up Orders " + identifiers.ToString() + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Orders |  Matching Dummy Pick Up Orders " + identifiers.ToString(), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return orders;
        }

        public List<Order> RetrieveOrdersFromRNA(out bool errorCaught, out string errorMessage, string[] identifiers)
        {
            List<Order> orders = null;


            errorCaught = false;
            errorMessage = string.Empty;
            string identifiersStrings = string.Join(" ", identifiers);
            _Logger.Debug("Retrieving RNAOrders the following DB Order Record Identifiers: " + identifiersStrings);
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifiers }
                        },

                        PropertyInclusionMode = PropertyInclusionMode.All,
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Order)
                    });
                if (retrievalResults.Items == null)
                {
                    string identifiersString = string.Join(" ", identifiers);
                    _Logger.Error("Retrieve Orders | Matching Orders " + identifiersString + " | Failed with a null result.");
                    errorCaught = true;
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    string identifiersString = string.Join(" ", identifiers);
                    _Logger.Debug("Retrieve Orders |  Orders Not Found for the following Order Identifiers: " + identifiersString);

                    orders = retrievalResults.Items.Cast<Order>().ToList();
                }
                else
                {
                    string identifiersString = string.Join(" ", identifiers);
                    _Logger.Error("Retrieve Orders |  Matching Orders Found For " + identifiersString + " | " + errorMessage);
                    orders = retrievalResults.Items.Cast<Order>().ToList();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {

                _Logger.Error("Retrieve Orders |  Matching Orders " + identifiers.ToString() + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                }
            }
            catch (Exception ex)
            {
                errorMessage = "Error Occured: " + ex.Message;
                _Logger.Error("Retrieve Orders |  Matching Orders " + identifiers.ToString() + errorMessage);
                errorCaught = true;

            }
            return orders;
        }

        public List<Order> RetrieveDummyRNAOrders(out bool errorCaught, out string fatalErrorMessage, string[] identifiers)
        {
            List<Order> orders = null;


            errorCaught = false;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifiers }
                        },


                        PropertyInclusionMode = PropertyInclusionMode.All,
                       
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Order)
                    });
                if (retrievalResults.Items == null)
                {
                    string identifiersString = string.Join(" ", identifiers);
                    _Logger.Error("Retrieve Dummy Orders | Matching Orders " + identifiersString + " | Failed with a null result.");
                    errorCaught = true;
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "Dummy Orders  do not exist.";
                    string identifiersString = string.Join(" ", identifiers);
                    _Logger.Error("Retrieve Dummy Orders  | Matching Orders " + identifiersString + " | Orders do not exist.");
                   
                    errorCaught = false;
                    orders = null;
                }
                else
                {
                    orders = retrievalResults.Items.Cast<Order>().ToList(); ;
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                _Logger.Error("Retrieve Dummy Orders  |  Matching Orders " + identifiers.ToString() + " | " + fatalErrorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                }
            }
            catch (Exception ex)
            {
                fatalErrorMessage = "Error Occured: " + ex.Message;
                _Logger.Error("Retrieve Dummy Orders  |  Matching Orders " + identifiers.ToString() + fatalErrorMessage);
                errorCaught = true;
            }
            return orders;
        }

        public void WriteRoutesAndUnOrdersToStagingTable(Dictionary<string, long> regionIdentifier, out ErrorLevel errorLevel, out string fatalErrorMessage, List<Route> saveRoutes, List<Order> unassignedOrders)
        {

            DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);
            string dateTime2Format = "YYYY-MM-dd hh:mm:ss.FFFFFFF";
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;

            try
            {
                foreach (Route route in saveRoutes)
                {
                    var regiondId = regionIdentifier.FirstOrDefault(x => x.Value == route.EntityKey).Key;
                    string routeStartTime = null;
                    string routeDesc = null;
                    string routeStage = DateTime.UtcNow.ToString(dateTime2Format);
                    string routeError = string.Empty;
                    //string routeStatus = "COMPLETE";

                    if (route.StartTime.Value != null)
                    {

                        routeStartTime = route.StartTime.Value.ToUniversalTime().ToString(dateTime2Format);
                    }
                    else
                    {
                        routeStartTime = null;
                        _Logger.ErrorFormat("The Start Time for Route {0} is null", route.Identifier);
                    }
                    if (route.Description != null)
                    {

                        routeDesc = route.Description;
                    }
                    else
                    {
                        routeDesc = null;
                        _Logger.ErrorFormat("The Description for Route {0} is null", route.Identifier);
                    }

                    foreach (Stop stop in route.Stops)
                    {
                        if (stop is ServiceableStop)
                        {
                            ServiceableStop thisStop = (ServiceableStop)stop;
                            if (thisStop.Actions != null)
                            {
                                string stopSequence = null;
                                if (thisStop.SequenceNumber.HasValue)
                                {

                                    stopSequence = thisStop.SequenceNumber.Value.ToString();
                                }
                                else
                                {
                                    stopSequence = null;
                                    _Logger.ErrorFormat("The Sequence Number for Route {0} is null", route.Identifier);
                                }
                                foreach (StopAction order in thisStop.Actions)
                                {

                                    //DBAccessor.InsertStagedRoute(regiondId, order.OrderIdentifier, route.Identifier, routeStartTime, routeDesc, stopSequence, routeStage, routeError, routeStatus);
                                    _Logger.DebugFormat("Route {0} Sucessfully written into STAGE_ROUTE table", route.Identifier);

                                }
                            }
                        }
                    }
                }

                foreach (Order order in unassignedOrders)
                {
                    string regiondId = regionIdentifier.FirstOrDefault(x => x.Value == order.RegionEntityKey).Key;
                    string orderError = string.Empty;
                    bool orderErrorCaught = false;
                    string orderStatus = "NEW";
                    string orderStage = DateTime.UtcNow.ToString(dateTime2Format);

                    DBAccessor.InsertStagedUnassignedOrders(regiondId, order.Identifier, orderStage, orderError, orderStatus, out orderError, out orderErrorCaught);

                    if (orderErrorCaught)
                    {
                        _Logger.Error("Error Writting Unassigned Orders to Staged Route Table | " + orderError);
                        errorLevel = ErrorLevel.Transient;
                    }
                    else
                    {
                        _Logger.DebugFormat("Unassigned Order {0} Sucessfully written into STAGE_ROUTE table", order.Identifier);
                    }


                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Write Routes Error | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Routes | " + ex.Message);
                errorLevel = ErrorLevel.Transient;
            }

        }


        public void RetrieveSLFromSTandSaveToRNA(Dictionary<string, long> regionEntityKeyDic, Dictionary<string, long> timeWindowTypes, Dictionary<string, long> servicetimeTypes, string regionId,
            out bool errorRetrieveSLFromStagingTable, out string errorRetrieveSLFromStagingTableMessage, out string fatalErrorMessage, out bool timeOut)
        {

            timeOut = false;
            ErrorLevel errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            errorRetrieveSLFromStagingTable = false;
            errorRetrieveSLFromStagingTableMessage = string.Empty;
            List<DBAccess.Records.StagedServiceLocationRecord> retrieveList = new List<DBAccess.Records.StagedServiceLocationRecord>();
            DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);
            List<ServiceLocation> checkedServiceLocationList = new List<ServiceLocation>();
            List<ServiceLocation> saveServiceLocations = new List<ServiceLocation>();
            List<ServiceLocation> rnaServiceLocations = new List<ServiceLocation>();

            try
            {
                //Get New Service Location Records from database
                retrieveList = DBAccessor.SelectAllStagedServiceLocations(regionId);
                List<DBAccess.Records.StagedServiceLocationRecord> checkedServiceLocationRecordList = retrieveList;

                if (retrieveList == null)// Database Service Locations Table Null
                {
                    errorRetrieveSLFromStagingTable = true;
                    _Logger.ErrorFormat("Error Retrieving New Locations from Database");
                }
                else if (retrieveList.Count == 0)//  Database Service Locations Table Empty
                {
                    errorRetrieveSLFromStagingTable = true;
                    errorRetrieveSLFromStagingTableMessage = String.Format("No New Staged Service Locations found in Staged Service Locations Table for {0}", regionId);
                    _Logger.ErrorFormat(errorRetrieveSLFromStagingTableMessage);

                }
                else
                {

                    List<ServiceLocation> serviceLocationsInRna = new List<ServiceLocation>();

                    //Check for Duplicates Records in Table
                    if (retrieveList.Count != retrieveList.Distinct(new DBAccess.Records.ServiceLocationComparer()).Count())
                    {
                        bool errorDeletingDuplicateServiceLocations = false;
                        string errorDeletingDuplicateServiceLocationsMessage = string.Empty;
                        //filter out duplicates
                        _Logger.DebugFormat("Duplicate Service Locations Found, Deleting them from Staged Service Locations Table");
                        checkedServiceLocationRecordList = retrieveList.Distinct(new DBAccess.Records.ServiceLocationComparer()).ToList(); //filter out duplicates
                        DBAccessor.DeleteDuplicatedServiceLocation(out errorDeletingDuplicateServiceLocationsMessage, out errorDeletingDuplicateServiceLocations);
                        if (errorDeletingDuplicateServiceLocations == true)
                        {
                            _Logger.ErrorFormat("Error Deleting Duplicate Service Locations: " + errorDeletingDuplicateServiceLocationsMessage);
                        }
                        else
                        {
                            _Logger.DebugFormat("Deleting Service Locations from Staged Service Locations Table Sucessful");
                        }
                    }
                    else
                    {
                        checkedServiceLocationRecordList = retrieveList;

                    }
                    //Check if address fields are blank
                    List<DBAccess.Records.StagedServiceLocationRecord> noneblankFieldRecords = new List<DBAccess.Records.StagedServiceLocationRecord>();
                    foreach (DBAccess.Records.StagedServiceLocationRecord record in checkedServiceLocationRecordList)
                    {
                        if (record.AddressLine1.Length == 0 || record.AddressLine1 == null || record.City.Length == 0 || record.City == null ||
                            record.State.Length == 0 || record.State == null || record.PostalCode.Length == 0 || record.PostalCode == null)
                        {
                            //This record has blanks. Add error and switch status to error 
                            string errorUpdatingServiceLocationMessage = string.Empty;
                            bool errorUpdatingServiceLocation = false;
                            _Logger.DebugFormat("Service location record {0} in region {1} has blank address fields. Error will be added to service location", record.ServiceLocationIdentifier, record.RegionIdentifier);
                            DBAccessor.UpdateServiceLocationStatus(record.RegionIdentifier, record.ServiceLocationIdentifier, "Service location has blank address fields",
                                "Error", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                            if (!errorUpdatingServiceLocation)
                            {
                                _Logger.DebugFormat("Location record {0} in region {1} updated successfully", record.ServiceLocationIdentifier, record.RegionIdentifier);
                            }
                            else
                            {
                                _Logger.DebugFormat("Error Updating Location record {0} in region {1}: {2}", record.ServiceLocationIdentifier, record.RegionIdentifier, errorUpdatingServiceLocationMessage);
                            }

                        }
                        else
                        {
                            noneblankFieldRecords.Add(record);
                        }
                    }


                    //Retrieve service locations with status of New
                    checkedServiceLocationRecordList = noneblankFieldRecords.FindAll(x => x.Status.ToUpper().Equals("NEW"));
                    string[] checkedSLId = new string[] { };
                    if (checkedServiceLocationRecordList.Count != 0)
                    {
                        checkedSLId = checkedServiceLocationRecordList.Select(x => x.ServiceLocationIdentifier).ToArray();

                        //Add serviceTimeType, timeWindowType, and region Entity Keys to Checked Service Locations
                        foreach (DBAccess.Records.StagedServiceLocationRecord location in checkedServiceLocationRecordList)
                        {

                            _Logger.DebugFormat("Retrieved {0} Location from Location Table", location.ServiceLocationIdentifier);
                            long serviceTimeTypeEntityKey = 0;
                            long timeWindowTypeEntityKey = 0;
                            long[] regionEntityKey = new long[1] { 0 };
                            var temp = (ServiceLocation)location;
                            _Logger.DebugFormat("Cast Location {0} as Service Location Object", temp.Identifier);

                            //Add SerivceTimeType, region and timewindowType entity keys

                            if (!servicetimeTypes.TryGetValue(location.ServiceTimeTypeIdentifier, out serviceTimeTypeEntityKey))
                            {
                                _Logger.ErrorFormat("No match found for Service Time Type with identifier {0} in RNA, Using Default Service Time Type", location.ServiceTimeTypeIdentifier);
                                temp.ServiceTimeTypeEntityKey = 0;
                            }
                            else
                            {
                                _Logger.DebugFormat("Adding Service Time Type {0} with Entity Key {1} to location ", location.ServiceTimeTypeIdentifier, serviceTimeTypeEntityKey, temp.Identifier);
                                temp.ServiceTimeTypeEntityKey = serviceTimeTypeEntityKey;
                            }
                            if (!timeWindowTypes.TryGetValue(location.ServiceWindowTypeIdentifier, out timeWindowTypeEntityKey))
                            {
                                _Logger.ErrorFormat("No match found for Time Window Type with identifier {0} in RNA, Using Default Time Window Type", location.ServiceTimeTypeIdentifier);
                                temp.ServiceTimeTypeEntityKey = 0;
                                _Logger.ErrorFormat("Adding Successful");
                            }
                            else
                            {
                                _Logger.DebugFormat("Adding Time Window Type {0} with Entity Key {1} to location ", location.ServiceWindowTypeIdentifier, timeWindowTypeEntityKey, temp.Identifier);
                                temp.TimeWindowTypeEntityKey = timeWindowTypeEntityKey;
                                _Logger.ErrorFormat("Adding Successful");
                            }

                            if (!regionEntityKeyDic.TryGetValue(location.RegionIdentifier, out regionEntityKey[0]))
                            {
                                _Logger.ErrorFormat("No match found for Region Entity Key with identifier {0} in RNA", location.RegionIdentifier);
                                temp.RegionEntityKeys = new long[1];
                                temp.RegionEntityKeys[0] = 0;
                            }
                            else
                            {
                                _Logger.DebugFormat("Adding Region {0} with Entity Key {1} to location", location.RegionIdentifier, regionEntityKey, temp.Identifier);
                                temp.RegionEntityKeys = new long[] { };
                                temp.RegionEntityKeys = regionEntityKey;
                                _Logger.ErrorFormat("Adding Successful");
                            }


                            checkedServiceLocationList.Add(temp);
                        }

                        //Retrieve Service Locations from RNA
                        //rnaServiceLocations = RetrieveServiceLocations(out errorLevel, out fatalErrorMessage, checkedSLId, false).ToList();



                        if (rnaServiceLocations == null || rnaServiceLocations.Count == 0) //No new Service Locations found, Add all service locations to RNA
                        {

                            Address[] newAddress = checkedServiceLocationList.Select(x => x.Address).ToArray();
                            //Geocode Address


                            //Look for best accuracy for geocode result

                            try
                            {
                                GeocodeResult[] newAddressGeocodeResult = Geocode(out errorLevel, out fatalErrorMessage, out timeOut, newAddress);

                                if (timeOut)
                                {
                                    _Logger.Error("Geocoding Addresses for Service Location Records has Timed Out");
                                }
                                else
                                {
                                    for (int y = 0; y < checkedServiceLocationList.Count; y++)
                                    {
                                        Dictionary<string, Coordinate> bestGeoCodeForLocation = new Dictionary<string, Coordinate>();

                                        if (errorLevel == ApexConsumer.ErrorLevel.None)
                                        {


                                            for (int j = 0; j < newAddressGeocodeResult[y].Results.Length; j++) //for the corresponding GeocodeCandidate for a location
                                            {

                                                for (int i = 0; i < GeocodeAccuracyDict.Count; i++) //Check all entries in Geocode Accuracy Dict in order
                                                {

                                                    string accuracyGeo = string.Empty;

                                                    if (GeocodeAccuracyDict.TryGetValue(i, out accuracyGeo)) //Get dict accuracy code from rank
                                                    {
                                                        if (accuracyGeo == newAddressGeocodeResult[y].Results[j].GeocodeAccuracy_Quality) // does candidate accuracy match ranked accuracy code?
                                                        {

                                                            bestGeoCodeForLocation.Add(newAddressGeocodeResult[y].Results[j].GeocodeAccuracy_Quality, newAddressGeocodeResult[y].Results[j].Coordinate);
                                                            break;
                                                        }
                                                    }


                                                }

                                            }

                                            //Get Best Coordinate
                                            Coordinate mostAccurateCordinate = new Coordinate();
                                            for (int i = 0; i <= GeocodeAccuracyDict.Count; i++) //Check all entries in Geocode Accuracy Dict in order
                                            {
                                                string accuracyGeo = string.Empty;
                                                if (GeocodeAccuracyDict.TryGetValue(i, out accuracyGeo)) //Get dict accuracy code in order
                                                {
                                                    Coordinate bestGeocodeCordinate = new Coordinate();

                                                    if (bestGeoCodeForLocation.TryGetValue(accuracyGeo, out bestGeocodeCordinate)) // get coordinate with the highest accuracy
                                                    {
                                                        checkedServiceLocationList[y].Coordinate = bestGeocodeCordinate;
                                                        checkedServiceLocationList[y].GeocodeAccuracy_GeocodeAccuracy = accuracyGeo;


                                                    }
                                                }


                                            }

                                            //Add location to save list with BU entity key
                                            checkedServiceLocationList[y].Action = ActionType.Add;
                                            checkedServiceLocationList[y].BusinessUnitEntityKey = _BusinessUnitEntityKey;
                                            saveServiceLocations.Add(checkedServiceLocationList[y]);
                                        }
                                        else
                                        {
                                            _Logger.Error("Error Occured Geocoding Addresses");
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _Logger.Error("An error has occured during Geocoding Service Locations" + ex.Message);
                            }

                        }





                        else  //Service Location Record have a corresponding Service Location in RNA
                        {
                            // Order Checked List and Returned RNA List
                            //List<ServiceLocation> newLocations = checkedServiceLocationList.Where(l => !rnaServiceLocations.Any(l2 => l2.Identifier == l.Identifier)).ToList();


                            checkedServiceLocationList.OrderBy(x => x.Identifier);
                            rnaServiceLocations.OrderBy(x => x.Identifier);

                            //
                            Address[] newAddress = checkedServiceLocationList.Select(x => x.Address).ToArray();
                            //Geocode Address
                            try
                            {
                                GeocodeResult[] newAddressGeocodeResult = Geocode(out errorLevel, out fatalErrorMessage, out timeOut, newAddress);

                                if (timeOut)
                                {
                                    _Logger.Error("Geocoding Addresses for Service Location Records has Timed Out");
                                }
                                else
                                {
                                    for (int y = 0; y < checkedServiceLocationList.Count; y++)
                                    {
                                        Dictionary<string, Coordinate> bestGeoCodeForLocation = new Dictionary<string, Coordinate>();

                                        if (errorLevel == ApexConsumer.ErrorLevel.None)
                                        {


                                            for (int j = 0; j < newAddressGeocodeResult[y].Results.Length; j++) //for the corresponding GeocodeCandidate for a location
                                            {

                                                for (int i = 0; i < GeocodeAccuracyDict.Count; i++) //Check all entries in Geocode Accuracy Dict in order
                                                {

                                                    string accuracyGeo = string.Empty;

                                                    if (GeocodeAccuracyDict.TryGetValue(i, out accuracyGeo)) //Get dict accuracy code from rank
                                                    {
                                                        if (accuracyGeo == newAddressGeocodeResult[y].Results[j].GeocodeAccuracy_Quality) // does candidate accuracy match ranked accuracy code?
                                                        {

                                                            bestGeoCodeForLocation.Add(newAddressGeocodeResult[y].Results[j].GeocodeAccuracy_Quality, newAddressGeocodeResult[y].Results[j].Coordinate);
                                                            break;
                                                        }
                                                    }


                                                }

                                            }






                                            //Get Best Coordinate
                                            Coordinate mostAccurateCordinate = new Coordinate();
                                            for (int i = 0; i < GeocodeAccuracyDict.Count; i++) //Check all entries in Geocode Accuracy Dict in order
                                            {
                                                string accuracyGeo = string.Empty;
                                                if (GeocodeAccuracyDict.TryGetValue(i, out accuracyGeo)) //Get dict accuracy code in order
                                                {
                                                    Coordinate bestGeocodeCordinate = new Coordinate();

                                                    if (bestGeoCodeForLocation.TryGetValue(accuracyGeo, out bestGeocodeCordinate)) // get coordinate with the highest accuracy
                                                    {
                                                        checkedServiceLocationList[y].Coordinate = bestGeocodeCordinate;
                                                        checkedServiceLocationList[y].GeocodeAccuracy_GeocodeAccuracy = accuracyGeo;


                                                    }
                                                }


                                            }

                                            //if the service location is in RNA, action is Update
                                            if (rnaServiceLocations.Any(l => l.Identifier == checkedServiceLocationList[y].Identifier))
                                            {
                                                checkedServiceLocationList[y].Action = ActionType.Update;
                                                checkedServiceLocationList[y].EntityKey = rnaServiceLocations.Where(l => l.Identifier == checkedServiceLocationList[y].Identifier).First().EntityKey;
                                            }
                                            else //if not action is add
                                            {
                                                checkedServiceLocationList[y].Action = ActionType.Add;
                                            }

                                            //Add location to save list with BU entity key
                                            checkedServiceLocationList[y].BusinessUnitEntityKey = _BusinessUnitEntityKey;
                                            saveServiceLocations.Add(checkedServiceLocationList[y]);
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _Logger.Error("An error has occured during Geocoding Service Locations" + ex.Message);
                            }

                            //Look for best accuracy for geocode result


                        }


                    }
                    else
                    {
                        saveServiceLocations = new List<ServiceLocation>();
                    }




                    //Save servicelocations list to RNA
                    try
                    {
                        if (saveServiceLocations.Count != 0)
                        {
                            foreach (ServiceLocation saveLocation in saveServiceLocations)
                            {
                                SaveResult[] saveLocationResults = SaveServiceLocations(out errorLevel, out fatalErrorMessage, new ServiceLocation[] { saveLocation });

                                if (errorLevel == ApexConsumer.ErrorLevel.Fatal)
                                {
                                    _Logger.Debug("Fatal Error Occured Saving Service Locations: " + fatalErrorMessage);


                                }
                                else if (errorLevel == ApexConsumer.ErrorLevel.Partial || errorLevel == ApexConsumer.ErrorLevel.Transient)
                                {



                                    foreach (SaveResult saveResult in saveLocationResults)
                                    {
                                        if (saveResult.Error != null)
                                        {
                                            var temp = (ServiceLocation)saveResult.Object;
                                            bool errorUpdatingServiceLocation = false;
                                            string errorUpdatingServiceLocationMessage = string.Empty;
                                            //var regionIdent = regionEntityKeyDic.Where(pair => pair.Value == temp.RegionEntityKeys[0]).Select(pair => pair.Key).FirstOrDefault();



                                            if (saveResult.Error.ValidationFailures != null)
                                            {
                                                foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                                {
                                                    _Logger.Debug("A Validation Error Occured While Saving Service Location. The " + validFailure.Property + " Property for Service Location " + temp.Identifier + " is not Valid");
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Error");
                                                    DBAccessor.UpdateServiceLocationStatus(_Region.Identifier, temp.Identifier, "Validation Error For Properties " + validFailure.Property + "See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                                    if (errorUpdatingServiceLocation)
                                                    {
                                                        _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Complete failed | " + errorUpdatingServiceLocationMessage);

                                                    }
                                                    else
                                                    {
                                                        _Logger.Debug("Updating Service Location " + temp.Identifier + " Status Succesfull");
                                                    }
                                                }
                                            }
                                            else if (saveResult.Error.Code.ErrorCode_Status == "DuplicateData")
                                            {
                                                _Logger.Debug("A Duplicate Save Data Error Occured While Saving Service Location " + temp.Identifier);
                                                _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Error");
                                                DBAccessor.UpdateServiceLocationStatus(_Region.Identifier, temp.Identifier, "Duplicate Save Data Error Occured While Saving Service Location ", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                                if (errorUpdatingServiceLocation)
                                                {
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Complete failed | " + errorUpdatingServiceLocationMessage);

                                                }
                                                else
                                                {
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status Succesfull");
                                                }
                                            }
                                            else if (saveResult.Error.Code.ErrorCode_Status == "NoResultsFound| ")
                                            {
                                                _Logger.Debug("An Update Error Occured While Saving Service Location " + temp.Identifier + ". The Updated Information Is The Same Information Found In RNA");
                                                DBAccessor.UpdateServiceLocationStatus(_Region.Identifier, temp.Identifier, "An Update Error Occured While Saving Service Location. The Updated Information Is The Same Information Found In RNA", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                                if (errorUpdatingServiceLocation)
                                                {
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Complete failed | " + errorUpdatingServiceLocationMessage);

                                                }
                                                else
                                                {
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status Succesfull");
                                                }
                                            }
                                            else
                                            {
                                                var temp3 = "An Error Occured While Saving Service Location. " + saveResult.Error.Code.ErrorCode_Status + " " + saveResult.Error.Detail;
                                                _Logger.Debug("An Error Occured While Saving Service Location " + temp.Identifier + ". The Error is the Following: " + saveResult.Error.Code.ErrorCode_Status + "| " + saveResult.Error.Detail);
                                                DBAccessor.UpdateServiceLocationStatus(_Region.Identifier, temp.Identifier, temp3, "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                                if (errorUpdatingServiceLocation)
                                                {
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Complete failed | " + errorUpdatingServiceLocationMessage);

                                                }
                                                else
                                                {
                                                    _Logger.Debug("Updating Service Location " + temp.Identifier + " Status Succesfull");
                                                }
                                            }




                                        }
                                    }
                                }
                                else if (errorLevel == ApexConsumer.ErrorLevel.None)
                                {
                                    foreach (SaveResult saveResult in saveLocationResults)
                                    {
                                        var temp = (ServiceLocation)saveResult.Object;
                                        bool errorUpdatingServiceLocation = false;
                                        string errorUpdatingServiceLocationMessage = string.Empty;
                                        _Logger.Debug("Service Location " + temp.Identifier + " Saved Successfully");

                                        var regionIdent = regionEntityKeyDic.Where(pair => pair.Value == temp.RegionEntityKeys[0]).Select(pair => pair.Key).FirstOrDefault();
                                        DBAccessor.UpdateServiceLocationStatus(regionIdent, temp.Identifier, "", "COMPLETE", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);

                                        if (errorUpdatingServiceLocation)
                                        {
                                            _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Complete failed | " + errorUpdatingServiceLocationMessage);
                                        }
                                        else
                                        {
                                            _Logger.Debug("Updating Service Location " + temp.Identifier + " Status To Complete Succesfull");
                                        }
                                    }

                                }

                            }
                        }
                        else
                        {
                            _Logger.Debug("No Service Location in Database to Update");
                        }

                    }
                    catch (Exception ex)
                    {
                        _Logger.Error(ex.Message);
                        errorRetrieveSLFromStagingTable = true;
                        errorRetrieveSLFromStagingTableMessage = ex.Message;
                    }
                }
            }

            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Service Location | " + errorMessage);

                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error(ex.Message);
                errorLevel = ErrorLevel.Transient;
            }




        }


     
        public List<Order> RetrieveOrdersFromStagingTable(Dictionary<string, long> RetrieveDepotsForRegion, string regionId, string staged, out bool errorRetrieveOrdersFromStagingTable, out string errorRetrieveOrdersFromStagingTableMessage)
        {

            errorRetrieveOrdersFromStagingTable = false;
            errorRetrieveOrdersFromStagingTableMessage = string.Empty;
            List<DBAccess.Records.StagedOrderRecord> retrieveListOrders = new List<DBAccess.Records.StagedOrderRecord>();
            DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);



            try
            {

                retrieveListOrders = DBAccessor.SelectStagedOrders(regionId, "False").ToList();

                if (retrieveListOrders == null)
                {
                    errorRetrieveOrdersFromStagingTable = true;
                    _Logger.ErrorFormat(errorRetrieveOrdersFromStagingTableMessage);
                    return null;

                }
                else if (retrieveListOrders.Count == 0)
                {
                    errorRetrieveOrdersFromStagingTable = true;
                    errorRetrieveOrdersFromStagingTableMessage = String.Format("No New Staged Orders found in Staged Order Table for {0}", regionId);
                    _Logger.ErrorFormat(errorRetrieveOrdersFromStagingTableMessage);
                    return retrieveListOrders.Cast<Order>().ToList();
                }
                else
                {
                    List<Order> stagedOrders = new List<Order>();
                    foreach (DBAccess.Records.StagedOrderRecord order in retrieveListOrders)
                    {
                        long orginDepotEntityKey = 0;

                        Order temp = (Order)order;



                        if (order.Status.ToUpper() == "NEW")
                        {
                            if (!RetrieveDepotsForRegion.TryGetValue(order.OriginDepotIdentifier, out orginDepotEntityKey))
                            {
                                _Logger.ErrorFormat("No match found for Orgin Depot with identifier {0} in RNA", order.OriginDepotIdentifier);
                                temp.RequiredRouteOriginEntityKey = 0;
                            }
                            else
                            {
                                temp.RequiredRouteOriginEntityKey = orginDepotEntityKey;
                            }

                            stagedOrders.Add(temp);
                        }
                    }
                    return stagedOrders;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error(ex.Message);
            }
            return null;
        }

      

        public List<Order> RetrieveDummyOrdersFromCSV(Dictionary<string, Depot> originDict, Dictionary<string, long> orderClassDict, string regionId, out bool errorRetrieveDummyOrdersFromCSV, out string errorRetrieveDummyOrdersFromCSVMessage)
        {
            //Retrieve Dummy Pickup Orders
            errorRetrieveDummyOrdersFromCSV = false;
            errorRetrieveDummyOrdersFromCSVMessage = string.Empty;
            List<Order> retrieveListDummyOrders = new List<Order>();
            string dummyCsvFilename = Config.DummyOrderCSVFile + regionId + "_DummyOrders.csv";

            //read csv file
            try
            {
                if (File.Exists(dummyCsvFilename))
                {
                    using (StreamReader sr = new StreamReader(dummyCsvFilename))
                    {
                        sr.ReadLine();
                        int i = 1;
                        while (!sr.EndOfStream)
                        {
                            Order temp = new Order();
                            string currentLine = sr.ReadLine();
                            string[] dummyOrderValue = currentLine.Split(',');
                            if(dummyOrderValue.Length !=11)
                            {
                                _Logger.DebugFormat("File {0} not in the Correct Format. Order of line {1} not added to RNA", dummyCsvFilename, i.ToString());
                                i++;
                                continue;
                            }
                            if (dummyOrderValue[0].Length == 0 || dummyOrderValue[0] == null || dummyOrderValue[8].Length == 0 || dummyOrderValue[8] == null || dummyOrderValue[9].Length == 0 || dummyOrderValue[9] == null || dummyOrderValue[10].Length == 0 || dummyOrderValue[10] == null)
                            {
                                _Logger.DebugFormat("Dummy order on line {0} has some missing information. Order not added to RNA", i.ToString());
                                i++;
                                continue;
                            }
                            TaskServiceWindowOverrideDetail[] serviceWindowOverride = new TaskServiceWindowOverrideDetail[0];
                            long orderClassEntity;
                            Depot routeDepot;
                            temp.Action = ActionType.Add;
                            temp.Identifier = dummyOrderValue[0];
                            temp.BeginDate = dummyOrderValue[10];
                           
                            temp.PlannedPickupQuantities = new Quantities
                            {
                                Size1 = Convert.ToDouble(dummyOrderValue[1]),
                                Size2 = Convert.ToDouble(dummyOrderValue[2]),
                                Size3 = Convert.ToDouble(dummyOrderValue[3])

                            };
                            TaskServiceWindowOverrideDetail serviceWindow = null;

                            if (dummyOrderValue[5] != string.Empty && dummyOrderValue[6] != string.Empty)
                            {
                                serviceWindow = new TaskServiceWindowOverrideDetail
                                {
                                    Action = ActionType.Add,
                                    DailyTimePeriod = new DailyTimePeriod
                                    {

                                        DayOfWeekFlags_DaysOfWeek = "All",//Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday",
                                        StartTime = dummyOrderValue[5],
                                        EndTime = dummyOrderValue[6],

                                    },

                                };

                                temp.Tasks = new Task[]
                                { new Task
                                {
                                LocationIdentifier =  dummyOrderValue[4],
                                PlannedQuantities = new Quantities
                                {
                                    Size1 = Convert.ToDouble(dummyOrderValue[1]),
                                    Size2 = Convert.ToDouble(dummyOrderValue[2]),
                                    Size3 = Convert.ToDouble(dummyOrderValue[3])
                                },
                                ServiceWindowOverrides = new TaskServiceWindowOverrideDetail[]{ serviceWindow },
                                TaskType_Type = "Pickup",

                                }
                                };
                            }
                            else
                            {
                                temp.Tasks = new Task[]
                                { new Task
                                {
                                LocationIdentifier =  dummyOrderValue[4],
                                PlannedQuantities = new Quantities
                                {
                                    Size1 = Convert.ToDouble(dummyOrderValue[1]),
                                    Size2 = Convert.ToDouble(dummyOrderValue[2]),
                                    Size3 = Convert.ToDouble(dummyOrderValue[3])
                                },
                                ServiceWindowOverrides = null,
                                TaskType_Type = "Pickup",

                                }
                                };
                            }
                            temp.PreferredRouteIdentifier = dummyOrderValue[7];


                            if (orderClassDict.TryGetValue(dummyOrderValue[8], out orderClassEntity))
                            {
                                temp.OrderClassEntityKey = orderClassEntity;
                            }
                            else
                            {
                                _Logger.ErrorFormat("No Order Classes Found for Order", temp.Identifier);

                            }

                            if (originDict.TryGetValue(dummyOrderValue[9], out routeDepot))
                            {
                                
                                temp.RequiredRouteDestinationEntityKey = routeDepot.EntityKey;
                            }
                            else
                            {
                                _Logger.ErrorFormat("No Orgin Depot Found for Order", temp.Identifier);
                                temp.RequiredRouteOriginEntityKey = null;
                            }




                            retrieveListDummyOrders.Add(temp);
                            i++;
                        }
                    }
                }
                else
                {
                    errorRetrieveDummyOrdersFromCSV = true;
                    errorRetrieveDummyOrdersFromCSVMessage = String.Format("No file found that matches {0}", dummyCsvFilename);
                    _Logger.ErrorFormat(errorRetrieveDummyOrdersFromCSVMessage);
                    return null;
                }

                if (retrieveListDummyOrders == null)
                {
                    errorRetrieveDummyOrdersFromCSV = true;
                    _Logger.ErrorFormat(errorRetrieveDummyOrdersFromCSVMessage);
                    return null;

                }
                else if (retrieveListDummyOrders.Count == 0)
                {
                    errorRetrieveDummyOrdersFromCSV = true;
                    errorRetrieveDummyOrdersFromCSVMessage = String.Format("No Dummy Orders found in the CSV file {0} for region {1}", regionId);
                    _Logger.ErrorFormat(errorRetrieveDummyOrdersFromCSVMessage);
                    return retrieveListDummyOrders;
                }

                return retrieveListDummyOrders;
            }
            catch (Exception ex)
            {
                _Logger.Error(ex.Message);
            }

            return null;
        }

        public ServiceLocation[] RetrieveServiceLocations(
            out bool errorCaught,
            out string fatalErrorMessage,
            string[] identifiers,
            bool retrieveIdentifierOnly = false)
        {
            ServiceLocation[] serviceLocations = null;
            errorCaught = false;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifiers }
                        },

                        PropertyInclusionMode = retrieveIdentifierOnly ? PropertyInclusionMode.AccordingToPropertyOptions : PropertyInclusionMode.All,
                        PropertyOptions = new ServiceLocationPropertyOptions
                        {
                            Identifier = true,
                            RegionEntityKeys = true,

                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.ServiceLocation)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", identifiers) + " | Failed with a null result.");
                    errorCaught = false;
                    serviceLocations = retrievalResults.Items.Cast<ServiceLocation>().ToArray();
                }
                else
                {
                    serviceLocations = retrievalResults.Items.Cast<ServiceLocation>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", identifiers) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", identifiers), ex);
                errorCaught = true;
            }
            return serviceLocations;
        }

        public ServiceLocation[] RetrieveServiceLocationsByRegion(
          out ErrorLevel errorLevel,
          out string fatalErrorMessage,
          long[] regionKeys,
          bool retrieveIdentifierOnly = false)
        {
            ServiceLocation[] serviceLocations = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        PropertyInclusionMode = retrieveIdentifierOnly ? PropertyInclusionMode.AccordingToPropertyOptions : PropertyInclusionMode.All,
                        PropertyOptions = new ServiceLocationPropertyOptions
                        {
                            Identifier = true,
                            RegionEntityKeys = true,
                            Address = true,
                            Coordinate = true,
                            PhoneNumber = true,
                            Description = true,
                            StandardInstructions = true


                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.ServiceLocation)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", regionKeys) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                    serviceLocations = retrievalResults.Items.Cast<ServiceLocation>().ToArray();
                }
                else
                {
                    serviceLocations = retrievalResults.Items.Cast<ServiceLocation>().ToArray();
                }


            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", regionKeys) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", regionKeys), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return serviceLocations;
        }

        public ServiceLocation RetrieveServiceLocation(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            string identifier,
            bool retrieveIdentifierOnly = false)
        {
            ServiceLocation serviceLocation = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifier }
                        },
                        //TODO
                        PropertyInclusionMode = retrieveIdentifierOnly ? PropertyInclusionMode.AccordingToPropertyOptions : PropertyInclusionMode.All,
                        PropertyOptions = new ServiceLocationPropertyOptions
                        {
                            Identifier = true,

                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.ServiceLocation)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Service Location | " + string.Join(" | ", identifier) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                    return null;
                }
                else
                {
                    var temp = retrievalResults.Items.Cast<ServiceLocation>().ToArray();
                    serviceLocation = temp[0];
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Service Location | " + string.Join(" | ", identifier) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Service Locations | " + string.Join(" | ", identifier), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return serviceLocation;
        }

        public SaveResult[] SaveDailyRoutingSessions(
            out bool errorCaught,
            out string fatalErrorMessage,
            DateTime[] startDates, string[] originIdentifiers, Order order)
        {
            SaveResult[] saveResults = null;
            errorCaught = false;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    originIdentifiers.Select(originIdentifier => new DailyRoutingSession
                    {
                        Action = ActionType.Add,
                        Description = originIdentifier,
                        NumberOfTimeUnits = 1,
                        RegionEntityKey = _Region.EntityKey,
                        SessionMode_Mode = Enum.GetName(typeof(Apex.SessionMode), Apex.SessionMode.Operational),
                        StartDate = startDates[0].ToString(DATE_FORMAT),
                        TimeUnit_TimeUnitType = Enum.GetName(typeof(TimeUnit), TimeUnit.Day),
                        DepotEntityKey = order.RequiredRouteOriginEntityKey

                    }).ToArray(),
                    new SaveOptions
                    {
                        //TODO
                        InclusionMode = PropertyInclusionMode.All,
                        ReturnInclusionMode = PropertyInclusionMode.All,
                        ReturnSavedItems = true
                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | Failed with a null result.");
                    errorCaught = false;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("SaveDailyRoutingSessions | " + startDates[i] + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorCaught = false;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("SaveDailyRoutingSessions | " + string.Join(" | ", startDates), ex);
                errorCaught = true;
            }
            return saveResults;
        }

        public SaveResult[] SaveOrders(out ErrorLevel errorLevel, out string fatalErrorMessage, OrderSpec[] orderSpecs)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.SaveOrders(
                    MainService.SessionHeader,
                    _RegionContext,
                    orderSpecs,
                    new SaveOptions
                    {
                        IgnoreEntityVersion = true,
                        InclusionMode = PropertyInclusionMode.All,
                        ReturnSavedItems = true,


                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("SaveOrders | " + ToString(orderSpecs[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        public SaveResult[] SaveRNAOrders(out bool errorCaught, out string fatalErrorMessage, OrderSpec[] orderSpecs)
        {
            SaveResult[] saveResults = null;
            errorCaught = false;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.SaveOrders(
                    MainService.SessionHeader,
                    _RegionContext,
                    orderSpecs,
                    new SaveOptions
                    {
                        IgnoreEntityVersion = true,
                        InclusionMode = PropertyInclusionMode.All,
                        ReturnSavedItems = true,
                        
                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " | Failed with a null result.");
                    errorCaught = false;
                }
                else
                {
                    return saveResults;
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                fatalErrorMessage = "SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " " + ex.Message;

                _Logger.Error(fatalErrorMessage);
                errorCaught = true;

            }
            return saveResults;
        }


        public SaveResult[] SaveServiceLocations(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            ServiceLocation[] serviceLocations)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;

            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    serviceLocations,
                    new SaveOptions
                    {
                        //TODO
                        InclusionMode = PropertyInclusionMode.All,
                        ReturnSavedItems = true,
                        IgnoreEntityVersion = true
                        //PropertyOptions = new ServiceLocationPropertyOptions
                        //{
                        //    BusinessUnitEntityKey = true,
                        //    CreatedInRegionEntityKey = true,
                        //    RegionEntityKeys = true,
                        //    Address = true,
                        //    Coordinate = true,
                        //    Description = true,
                        //    GeocodeAccuracy_GeocodeAccuracy = true,
                        //    Identifier = true,
                        //    PhoneNumber = true,
                        //    WorldTimeZone_TimeZone = true,
                        //    Priority = true,
                        //    ServiceTimeTypeEntityKey = true,
                        //    TimeWindowTypeEntityKey = true,





                        // }
                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("SaveServiceLocations | " + ToString(serviceLocations[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }



        public SaveResult SaveRNAServiceLocations(
                   out bool errorCaught,
                   out string errorMessage,
                   ServiceLocation[] serviceLocations)
        {
            SaveResult[] saveResults = null;
            errorCaught = false;
            errorMessage = string.Empty;

            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    serviceLocations,
                    new SaveOptions
                    {
                        //TODO
                        InclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        ReturnSavedItems = true,
                        IgnoreEntityVersion = true,
                        ReturnPropertyOptions = new ServiceLocationPropertyOptions
                        {
                            Identifier = true
                        },
                        ReturnInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new ServiceLocationPropertyOptions
                        {
                            BusinessUnitEntityKey = true,
                            CreatedInRegionEntityKey = true,
                            RegionEntityKeys = true,
                            Address = true,
                            Description = true,
                            Identifier = true,
                            PhoneNumber = true,
                            WorldTimeZone_TimeZone = true,
                            ServiceTimeTypeEntityKey = true,
                            TimeWindowTypeEntityKey = true,
                            DayOfWeekFlags_DeliveryDays = true,
                            Coordinate = true,
                            GeocodeAccuracy_GeocodeAccuracy = true,
                            GeocodeAttentionReason_GeocodeAttentionReason = true,
                            GeocodeMethod_GeocodeMethod = true, 

                           

                        },


                    });
                if (saveResults == null)
                {
                    errorMessage = "SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " | Failed with a null result.";
                    _Logger.Error(errorMessage);
                    errorCaught = true;

                }

            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorFEMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    errorMessage = errorFEMessage;
                }
            }
            catch (Exception ex)
            {
                errorMessage = "SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " " + ex;

                _Logger.Error(errorMessage);
                errorCaught = true;
            }
            return saveResults[0];
        }

        public Dictionary<string, long> RetrieveRegionEntityKey(out ErrorLevel errorLevel, out string fatalErrorMessage)
        {
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            Dictionary<string, long> regionEntityKeyDic = new Dictionary<string, long>();
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.RetrieveRegionsGrantingPermissions(
                    MainService.SessionHeader, new RolePermission[] { }, false);

                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Regions failed.");
                    errorLevel = ErrorLevel.Transient;

                }
                else if (retrievalResults.Items.Length == 0)
                {
                    Console.WriteLine("No Regions exist.");
                    errorLevel = ErrorLevel.Fatal;
                    return null;
                }
                else
                {

                    regionEntityKeyDic = retrievalResults.Items.Cast<Region>().ToList().ToDictionary(x => x.Identifier, x => x.EntityKey);



                    return regionEntityKeyDic;

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Regions for the following business unit| " + string.Join(" | ", _BusinessUnitEntityKey.ToString()) + " | " + errorMessage);

                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;

                }

            }
            catch (Exception ex)
            {
                _Logger.ErrorFormat("Retrive Regions | {0}", ex.Message);

            }

            return null;
        }

        public Dictionary<string, long> RetrieveServiceTimeEntityKey(out ErrorLevel errorLevel, out string fatalErrorMessage)
        {
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    new SingleRegionContext
                    {
                        BusinessUnitEntityKey = _RegionContext.BusinessUnitEntityKey,
                        RegionEntityKey = _Region.EntityKey
                    },
                    new RetrievalOptions
                    {
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new ServiceTimeTypePropertyOptions
                        {
                            Identifier = true
                        },

                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.ServiceTimeType)
                    }

                    );
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Service Time Types for All Regions Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;

                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "ServiceTime Types does not exist.";
                    _Logger.Error("ServiceTime Types does not exist for Multiple Regions | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Fatal;
                }
                else
                {
                    Dictionary<string, long> temp = new Dictionary<string, long>();
                    List<ServiceTimeType> temp2 = retrievalResults.Items.Cast<ServiceTimeType>().ToList();
                    foreach (ServiceTimeType serviceTime in temp2)
                    {
                        if (!temp.ContainsKey(serviceTime.Identifier))
                        {
                            temp.Add(serviceTime.Identifier, serviceTime.EntityKey);
                            _Logger.DebugFormat("Added ServiceTime Type {0} to Dict", serviceTime.Identifier);
                        }
                        else
                        {
                            _Logger.DebugFormat("Duplicate Service Time Type Identifier found {0}. Will Use first one as entity Key", serviceTime.Identifier);
                        }
                    }

                    return temp;

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Service Time Type Entity Keys" + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;

                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }

            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Service Time Type Entity Key | " + ex);

            }
            return null;
        }

        public Dictionary<string, long> RetrieveTimeWindowEntityKey(out ErrorLevel errorLevel, out string fatalErrorMessage)
        {
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                     new MultipleRegionContext
                     {
                         BusinessUnitEntityKey = _RegionContext.BusinessUnitEntityKey,
                         Mode = MultipleRegionMode.All
                     },
                    new RetrievalOptions
                    {

                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new TimeWindowTypePropertyOptions
                        {
                            Identifier = true,


                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.TimeWindowType)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Time Window Type Dict | Failed with a null result.");

                }
                else
                {


                    Dictionary<string, long> temp = new Dictionary<string, long>();
                    List<TimeWindowType> temp2 = retrievalResults.Items.Cast<TimeWindowType>().ToList();
                    foreach (TimeWindowType timeWindow in temp2)
                    {
                        if (!temp.ContainsKey(timeWindow.Identifier))
                        {
                            temp.Add(timeWindow.Identifier, timeWindow.EntityKey);
                            _Logger.DebugFormat("Added Time Window Type {0} to Dict", timeWindow.Identifier);
                        }
                        else
                        {
                            _Logger.DebugFormat("Duplicate Time Window Type Identifier found {0}. Will Use first one as entity Key", timeWindow.Identifier);
                        }
                    }

                    return temp;



                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Time Window Type Type Entity Key | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;

                }

            }
            catch (Exception ex)
            {
                _Logger.ErrorFormat("Error Retrieving Service Time Type Entity Key | ", ex.Message);

            }
            return null;
        }

        public Dictionary<string, Depot> RetrieveDepotsForRegion(out ErrorLevel errorLevel, out string fatalErrorMessage, long regionEntityKey)
        {
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    new SingleRegionContext
                    {
                        BusinessUnitEntityKey = _RegionContext.BusinessUnitEntityKey,
                        RegionEntityKey = regionEntityKey

                    },
                    new RetrievalOptions
                    {
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new DepotPropertyOptions
                        {
                            Identifier = true,
                            WorldTimeZone_TimeZone = true


                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Depot)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve All Depots | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;

                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "Depots does not exist.";
                    _Logger.Error("Depots does not exist for Multiple Regions | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Fatal;
                }
                else
                {
                    return retrievalResults.Items.Cast<Depot>().ToDictionary(x => x.Identifier, y => y);

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Depot Entity Keys | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;

                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }

            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Depot Entity Key |  " + ex);

            }
            return null;
        }

        public Dictionary<string, long> RetrieveOrderClassesDict(out ErrorLevel errorLevel, out string fatalErrorMessage)
        {
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    new MultipleRegionContext
                    {
                        BusinessUnitEntityKey = _RegionContext.BusinessUnitEntityKey,
                        Mode = MultipleRegionMode.All
                    },
                    new RetrievalOptions
                    {
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new OrderClassPropertyOptions
                        {
                            Identifier = true

                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.OrderClass)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve All Order Classes | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;

                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "Order Classes does not exist.";
                    _Logger.Error("Order Classes does not exist for Multiple Regions | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Fatal;
                }
                else
                {
                    Dictionary<string, long> temp = new Dictionary<string, long>();
                    List<OrderClass> temp2 = retrievalResults.Items.Cast<OrderClass>().ToList();
                    foreach (OrderClass orderClass in temp2)
                    {
                        if (!temp.ContainsKey(orderClass.Identifier))
                        {
                            temp.Add(orderClass.Identifier, orderClass.EntityKey);
                            _Logger.DebugFormat("Added Order Class {0} to Dict", orderClass.Identifier);
                        }
                        else
                        {
                            _Logger.DebugFormat("Duplicate Order Class Identifier found {0}. Will Use first one as entity Key", orderClass.Identifier);
                        }
                    }

                    return temp;

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Order Classes Entity Keys | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;

                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }

            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Order Classes Entity Key |  " + ex);

            }
            return null;
        }

        public Route RetrieveRoute(out bool errorCaught, out string errorMessage, string RouteID, DateTime sessionDate, string sessionDescription)
        {
            errorCaught = false;
            errorMessage = string.Empty;
           
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    new SingleRegionContext
                    {
                        BusinessUnitEntityKey = _RegionContext.BusinessUnitEntityKey,
                        RegionEntityKey = _Region.EntityKey

                    },
                    new RetrievalOptions
                    {
                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "Identifier"},
                                    Right = new ValueExpression { Value = RouteID}

                                },

                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "RoutingSessionDescription"},
                                    Right = new ValueExpression { Value = sessionDescription}

                                },

                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "RoutingSessionDate"},
                                    Right = new ValueExpression { Value = sessionDate}

                                },
                            }
                        },



                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new RoutePropertyOptions
                        {
                            Identifier = true,
                            RoutingSessionEntityKey = true,
                            RoutingSessionDate = true,
                            OriginDepotIdentifier = true,
                            RoutingSessionDescription = true

                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Route)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.ErrorFormat("Retrieve Routes {0} | Failed with a null result.", RouteID);
                    errorCaught = true;

                }
                else if (retrievalResults.Items.Length == 0)
                {
                    errorMessage = string.Format("Route {0} does not exist.", RouteID);
                    _Logger.Error(errorMessage);
                    errorCaught = false;
                }
                else
                {

                    return retrievalResults.Items.Cast<Route>().DefaultIfEmpty(null).FirstOrDefault();


                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.ErrorFormat("Retrieve Route {0} failed| ", errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;

                }
                else
                {
                    errorCaught = true;
                }

            }
            catch (Exception ex)
            {
                _Logger.ErrorFormat("Error Retrieving Route {0] |  " + ex, RouteID);

            }
            return null;
        }

        public Order[] GetOrdersToUnassignOrDeleteInRNA(out ErrorLevel errorLevel, out string fatalErrorMessage, out string regularErrorMessage, string[] identifier)
        {
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            regularErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    new SingleRegionContext
                    {
                        BusinessUnitEntityKey = _RegionContext.BusinessUnitEntityKey,
                        RegionEntityKey = _Region.EntityKey

                    },
                    new RetrievalOptions
                    {
                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifier }
                        },
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new OrderPropertyOptions
                        {
                            Identifier = true,
                            RegionEntityKey = true,
                            Tasks = true,
                            UnassignedOrderGroupEntityKey = true




                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Order)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve All Orders | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                    regularErrorMessage = "Retrieve All Orders | Failed with a null result.";
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    regularErrorMessage = "Orders does not exist.";
                    _Logger.Error("Orders does not exist for Regions | " + regularErrorMessage);
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    return retrievalResults.Items.Cast<Order>().ToArray();

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Order Classes Entity Keys | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;

                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }

            }
            catch (Exception ex)
            {
                _Logger.Error("Retrieve Order Classes Entity Key |  " + ex);

            }
            return null;
        }

        public SaveResult[] DeleteOrder(out ErrorLevel errorLevel, out string fatalErrorMessage, Order[] deleteOrders)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {

                saveResults = _RoutingServiceClient.Save(
                MainService.SessionHeader,
                _RegionContext,
                deleteOrders,
                new SaveOptions
                {
                    InclusionMode = PropertyInclusionMode.All,
                    IgnoreEntityVersion = true
                });

                if (saveResults == null)
                {
                    _Logger.Error("DeleteOrders | " + string.Join(" | ", deleteOrders.Select(deleteOrder => ToString(deleteOrder))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("DeleteOrders | " + ToString(deleteOrders[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
                if (saveResults[0].Error != null)
                {
                    throw new Exception("Delete Order failed with Error: " + saveResults[0].Error.Code + " | " + saveResults[0].Error.Detail);
                }



            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("DeleteOrders | " + string.Join(" | ", deleteOrders.Select(deleteOrder => ToString(deleteOrder))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("DeleteOrders | " + string.Join(" | ", deleteOrders.Select(deleteOrder => ToString(deleteOrder))), ex);
                errorLevel = ErrorLevel.Transient;
            }


            return saveResults;


        }

        public SaveResult[] DeleteRNAOrder(out bool errorCaught, out string errorMessage, Order[] deleteOrders)
        {
            SaveResult[] saveResults = null;
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {

                saveResults = _RoutingServiceClient.Save(
                MainService.SessionHeader,
                _RegionContext,
                deleteOrders,
                new SaveOptions
                {
                    InclusionMode = PropertyInclusionMode.All,
                    IgnoreEntityVersion = true
                });

                if (saveResults == null)
                {
                    _Logger.Error("DeleteOrders | " + string.Join(" | ", deleteOrders.Select(deleteOrder => ToString(deleteOrder))) + " | Failed with a null result.");
                    errorCaught = true;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("DeleteOrders | " + ToString(deleteOrders[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorCaught = false;
                        }
                    }
                }
               



            }
            catch (FaultException<TransferErrorCode> tec)
            {

                _Logger.Error("DeleteOrders | " + string.Join(" | ", deleteOrders.Select(deleteOrder => ToString(deleteOrder))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                }
            }
            catch (Exception ex)
            {
                errorMessage = "DeleteOrders | " + string.Join(" | ", deleteOrders.Select(deleteOrder => ToString(deleteOrder))) + " " + ex.Message;
                errorCaught = true;
                _Logger.Error(errorMessage);
            }


            return saveResults;


        }

        public ManipulationResult UnassignedOrders(out ErrorLevel errorLevel, out string fatalErrorMessage, Order[] unassignOrders)
        {
            ManipulationResult unassignResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {

                unassignResults = _RoutingServiceClient.UnassignOrders(
                MainService.SessionHeader,
                _RegionContext,
                unassignOrders.Select(x => new DomainInstance
                {
                    EntityKey = x.EntityKey,
                    Version = x.Version
                }).ToArray(),
                new RemoveOrderOptions(), new RouteRetrievalOptions[] { }

                );


                if (unassignResults == null)
                {

                    _Logger.Error("Unassign Orders | " + string.Join(" | ", unassignOrders.Select(unassignOrder => ToString(unassignOrder))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < unassignResults.Errors.Length; i++)
                    {
                        if (unassignResults.Errors[i] != null)
                        {
                            _Logger.Error("Unassign Order | " + unassignResults.Errors[i].OrderEntityKey.ToString() + " | Failed with Error: " + unassignResults.Errors[i].Reason.ErrorCode_Status);
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }




            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("UnassignOrders | " + string.Join(" | ", unassignOrders.Select(unassignOrder => ToString(unassignOrder))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("DeleteOrders | " + string.Join(" | ", unassignOrders.Select(unassignOrder => ToString(unassignOrder))), ex);
                errorLevel = ErrorLevel.Transient;
            }


            return unassignResults;


        }

        public DailyPass RetrieveRoutingSessionPass(
          out bool errorCaught,
          out string fatalErrorMessage,
          long passSessionEntityKey, string passIdentifier, long regionEntity)
        {

            errorCaught = false;
            fatalErrorMessage = string.Empty;
            DailyPass dailySessionPass = null;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "Identifier" },
                                    Right = new ValueExpression { Value = passIdentifier }
                                },
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "SessionEntityKey" },
                                    Right = new ValueExpression { Value = passSessionEntityKey }
                                },
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "RegionEntityKey" },
                                    Right = new ValueExpression { Value =  regionEntity }
                                },
                            },
                        },


                        PropertyInclusionMode = PropertyInclusionMode.All,
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.DailyPass)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Daily Pass | " + string.Join(" | ", passIdentifier) + " | Failed with a null result.");
                    errorCaught = true;
                    return null;
                }
                else
                {
                    dailySessionPass = retrievalResults.Items.Cast<DailyPass>().DefaultIfEmpty(null).FirstOrDefault();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Daily Pass | " + string.Join(" | ", passIdentifier) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                fatalErrorMessage = "Retrieve Daily Pass | " + string.Join(" | ", passIdentifier) + ex.Message;
                _Logger.Error(fatalErrorMessage);
                errorCaught = true;
            }
            return dailySessionPass;
        }

        public PassTemplate RetrievePassTemplate(
         out bool errorCaught,
         out string fatalErrorMessage,
         string passIdentifier, long regionEntity)
        {

            errorCaught = false;
            fatalErrorMessage = string.Empty;
            PassTemplate passTemplate = null;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "Identifier" },
                                    Right = new ValueExpression { Value = passIdentifier }
                                },

                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "RegionEntityKey" },
                                    Right = new ValueExpression { Value =  regionEntity }
                                },
                            },
                        },


                        PropertyInclusionMode = PropertyInclusionMode.All,
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.DailyPassTemplate)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("Retrieve Daily Pass Template | " + string.Join(" | ", passIdentifier) + " | Failed with a null result.");
                    errorCaught = false;
                    return null;
                }
                else
                {
                    passTemplate = retrievalResults.Items.Cast<DailyPassTemplate>().DefaultIfEmpty(null).FirstOrDefault();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Retrieve Daily Pass Template | " + string.Join(" | ", passIdentifier) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                fatalErrorMessage = "Retrieve Daily Pass Template | " + string.Join(" | ", passIdentifier) + ex.Message;
                _Logger.Error(fatalErrorMessage);
                errorCaught = true;
            }
            return passTemplate;
        }


        public SaveResult[] CreateDailyPassforSession(
          out bool errorCaught,
          out string fatalErrorMessage,
          DailyRoutingSession routingSession, DailyPass dailySessionPass, long regionEntity, long equipmentTypeEntityKey)
        {

            errorCaught = false;
            fatalErrorMessage = string.Empty;
            SaveResult[] saveResult = new SaveResult[] { };
            try
            {
                saveResult = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    new AggregateRootEntity[] { dailySessionPass },
                    new SaveOptions
                    {
                        InclusionMode = PropertyInclusionMode.All,
                        ReturnSavedItems = true,
                        IgnoreEntityVersion = true,

                    }
                 );

            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Saving Daily Pass Error | " + string.Join(" | ", dailySessionPass.Identifier) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                fatalErrorMessage = "Saving Daily Pass Error | " + string.Join(" | ", dailySessionPass.Identifier) + ex.Message;
                _Logger.Error(fatalErrorMessage);
                errorCaught = true;
            }
            return saveResult;
        }


        public DailyRoutingSession[] RetrieveDailyRoutingSessionwithOrigin(
           out bool errorCaught,
           out string fatalErrorMessage,
           DateTime startDate, string originDepotsId)
        {
            DailyRoutingSession[] dailyRoutingSessions = null;
            errorCaught = false;
            fatalErrorMessage = string.Empty;

           
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {

                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "StartDate" },
                                    Right = new ValueExpression { Value = startDate }
                                },
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "Description" },
                                    Right = new ValueExpression { Value = originDepotsId }
                                },
                            },
                        },


                        PropertyInclusionMode = PropertyInclusionMode.All,
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.DailyRoutingSession)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDate) + " | Failed with a null result.");
                    errorCaught = true;
                }
                else
                {
                    dailyRoutingSessions = retrievalResults.Items.Cast<DailyRoutingSession>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDate) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDate), ex);
                errorCaught = true;
            }
            return dailyRoutingSessions;
        }

     

        public ManipulationResult UnassignOrders2(
            out bool errorCaught,
            out string fatalErrorMessage,
            Order[] orders)
        {
            ManipulationResult manipulationResult = null;
            errorCaught = false;
            fatalErrorMessage = string.Empty;
            try
            {
                manipulationResult = _RoutingServiceClient.UnassignOrders(
                    MainService.SessionHeader,
                    _RegionContext,
                    orders.Select(order => new DomainInstance
                    {
                        EntityKey = order.EntityKey,
                        Version = order.Version
                    }).ToArray(),
                    new RemoveOrderOptions(),
                    new RouteRetrievalOptions[] { });
                if (manipulationResult == null)
                {
                    _Logger.Error("UnassignOrders | " + string.Join(" | ", orders.Select(order => ToString(order))) + " | Failed with a null result.");
                    errorCaught = false ;
                }
                else if (manipulationResult.Errors != null)
                {
                    foreach (ManipulationResult.ManipulationError manipulationError in manipulationResult.Errors)
                    {
                        _Logger.Error("UnassignOrders | Failed with Error: " + ToString(manipulationError));
                        errorCaught = false;
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("UnassignOrders | " + string.Join(" | ", orders.Select(order => ToString(order))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("UnassignOrders | " + string.Join(" | ", orders.Select(order => ToString(order))), ex);
                errorCaught = true;
            }
            return manipulationResult;
        }



        public SaveResult[] DeleteOrders(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            Order[] orders)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {

                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    orders.Select(order => new Order
                    {
                        Action = ActionType.Delete,
                        EntityKey = order.EntityKey,
                        Version = order.Version
                    }).ToArray(),
                    new SaveOptions
                    {
                        InclusionMode = PropertyInclusionMode.All
                    });
                if (saveResults == null)
                {
                    _Logger.Error("DeleteOrders | " + string.Join(" | ", orders.Select(order => ToString(order))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("DeleteOrders | " + ToString(orders[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("DeleteOrders | " + string.Join(" | ", orders.Select(order => ToString(order))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("DeleteOrders | " + string.Join(" | ", orders.Select(order => ToString(order))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        public SaveRouteResult CreateRNARoute(
           out bool errorCaught,
           out string errorMessage,
           SaveRouteArgs routeArgs
          )
        {
            SaveRouteResult saveResults = null;
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {

                saveResults = _RoutingServiceClient.CreateRoute(
                    MainService.SessionHeader,
                    _RegionContext,
                    routeArgs
                    );
                if (saveResults == null)
                {
                    errorMessage = "Create Route  | " + routeArgs.Identifier + " | Failed with a null result.";
                    _Logger.Error(errorMessage);
                    errorCaught = true;
                }
                else
                {

                    if (saveResults.Error != null)
                    {
                        _Logger.Error("Create Route | " + routeArgs.Identifier + " | Failed with Error: " + ToString(saveResults.Error));
                        errorCaught = false;
                    }

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Create Route  | " + routeArgs.Identifier + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;

                }
            }
            catch (Exception ex)
            {
                errorMessage = "Save Route | " + routeArgs.Identifier + " | Exception Error" + ex.Message;
                _Logger.Error(errorMessage);
                errorCaught = true;
            }
            return saveResults;
        }


        public ManipulationResult AddOrderToRoute(
           out bool errorCaught,
           out string errorMessage,
           OnRouteAutomaticPlacement placementOptions,
           RouteRetrievalOptions options,
           Order order)
        {
            ManipulationResult saveResults = null;
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {

                saveResults = _RoutingServiceClient.MoveUnassignedOrderGroupsToBestPosition(
                    MainService.SessionHeader,
                    _RegionContext,
                    new DomainInstance[]
                    {
                        new DomainInstance
                        {
                            EntityKey = (long)order.UnassignedOrderGroupEntityKey,
                            Version = order.Version
                        }
                    }, placementOptions
                   ,
                    new RouteRetrievalOptions[] { options }

                    );
                if (saveResults == null)
                {
                    errorMessage = String.Format("Assiging Order {0} to Route {1} Failed with a null result", order.Identifier, order.PreferredRouteIdentifier);
                    _Logger.Error(errorMessage);
                    errorCaught = true;
                }
                else
                {

                    if (saveResults.Errors != null)
                    {
                        foreach (ManipulationResult.ManipulationError error in saveResults.Errors)
                        {
                            _Logger.ErrorFormat("Assiging Order {0} to Route {1} Failed | {2}", order.Identifier, order.PreferredRouteIdentifier, error.Reason.ErrorCode_Status);
                        }

                        errorCaught = false;
                    }

                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.ErrorFormat("Moving Order {0} to Route {1} | " + errorMessage, order.Identifier, order.PreferredRouteIdentifier);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorCaught = true;
                }
                else
                {
                    errorCaught = true;

                }
            }
            catch (Exception ex)
            {
                errorMessage = String.Format("Moving Order {0} to Route {1} | " + ex.Message, order.Identifier, order.PreferredRouteIdentifier);
                _Logger.Error(errorMessage);
                errorCaught = true;
            }
            return saveResults;
        }



        //public void RetrieveOrdersandSaveToRNA(Dictionary<string, long> regionEntityKeyDic, Dictionary<string, long> orginDepotTypes, Dictionary<string, long> orderClassTypes, string regionId,
        //   out bool errorRetrieveAndSavingOrdersFromStagingTable, out string errorRetrieveAndSavingOrdersFromStagingTableMessage, out string fatalErrorMessage, out bool timeOut)
        //{

        //    timeOut = false;
        //    ErrorLevel errorLevel = ErrorLevel.None;
        //    fatalErrorMessage = string.Empty;
        //    errorRetrieveAndSavingOrdersFromStagingTable = false;
        //    errorRetrieveAndSavingOrdersFromStagingTableMessage = string.Empty;
        //    List<DBAccess.Records.StagedOrderRecord> retrieveList = new List<DBAccess.Records.StagedOrderRecord>();
        //    DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);
        //    List<DBAccess.Records.StagedOrderRecord> checkedOrderRecordList = new List<DBAccess.Records.StagedOrderRecord>();
        //    List<Order> saveOrders = new List<Order>();
        //    List<Order> rnaOrders = new List<Order>();

        //    List<DailyRoutingSession> saveRoutingSession = new List<DailyRoutingSession>();

        //    try
        //    {
        //        //Get Order Records from database
        //        retrieveList = DBAccessor.SelectStagedOrders(regionId, "False");
        //        List<DBAccess.Records.StagedOrderRecord> checkedStagedOrdersList = retrieveList;
        //        List<DBAccess.Records.StagedOrderRecord> deleteOrderRecordList = DBAccessor.SelectStagedOrders(regionId, "True");

        //        if (retrieveList == null & deleteOrderRecordList == null)// Database Staged Orders Table Null Might Delete this 
        //        {
        //            errorRetrieveAndSavingOrdersFromStagingTable = true;
        //            _Logger.ErrorFormat(errorRetrieveAndSavingOrdersFromStagingTableMessage);


        //        }
        //        else if (retrieveList.Count == 0 && deleteOrderRecordList.Count == 0)//  Database Orders Table Empty
        //        {
        //            errorRetrieveAndSavingOrdersFromStagingTable = true;
        //            errorRetrieveAndSavingOrdersFromStagingTableMessage = String.Format("No Orders found in Staged Orders table for {0}", regionId);
        //            _Logger.ErrorFormat(errorRetrieveAndSavingOrdersFromStagingTableMessage);


        //        }
        //        else //Table has orders for region
        //        {
        //            List<Order> OrdersInRna = new List<Order>();
        //            List<Order> checkedOrdersList = new List<Order>();

        //            //Check for Duplicates Records in Table
        //            if (retrieveList.Count != retrieveList.Distinct(new DBAccess.Records.OrderComparer()).Count())
        //            {
        //                checkedOrderRecordList = retrieveList.Distinct(new DBAccess.Records.OrderComparer()).ToList(); //filter out duplicates

        //                bool errorDeletingDuplicateOrders = false;
        //                string errorDeletingDuplicateOrdersMessage = string.Empty;

        //                _Logger.DebugFormat("Duplicate Orders Found, Deleting them from Orders Table");

        //                DBAccessor.DeleteDuplicatedOrders(out errorDeletingDuplicateOrdersMessage, out errorDeletingDuplicateOrders);
        //                if (errorDeletingDuplicateOrders == true)
        //                {
        //                    _Logger.ErrorFormat("Error Deleting Duplicate Orders: " + errorDeletingDuplicateOrdersMessage);
        //                }
        //                else
        //                {
        //                    _Logger.DebugFormat("Deleting Orders from Staged Orders Table Sucessful");
        //                }
        //            }
        //            else
        //            {
        //                checkedOrderRecordList = retrieveList;

        //            }

        //            //check for blank order identififer fields
        //            List<DBAccess.Records.StagedOrderRecord> noneblankFieldRecords = new List<DBAccess.Records.StagedOrderRecord>();
        //            foreach (DBAccess.Records.StagedOrderRecord record in checkedOrderRecordList)
        //            {
        //                if (record.OrderIdentifier == null || record.OrderIdentifier.Length == 0)
        //                {
        //                    //This record has blanks. Add error and switch status to error 
        //                    string errorUpdatingOrderStatusMessage = string.Empty;
        //                    bool errorUpdatingOrderStatus = false;
        //                    _Logger.DebugFormat("Order record {0} in region {1} has a blank Order Identifier field. Error will be added to Order Record", record.OrderIdentifier, record.RegionIdentifier);
        //                    DBAccessor.UpdateOrderStatus(record.RegionIdentifier, record.OrderIdentifier, "Order has blank order identifier field",
        //                        "Error", out errorUpdatingOrderStatusMessage, out errorUpdatingOrderStatus);
        //                    if (!errorUpdatingOrderStatus)
        //                    {
        //                        _Logger.DebugFormat("Order record {0} in region {1} updated successfully", record.OrderIdentifier, record.RegionIdentifier);
        //                    }
        //                    else
        //                    {
        //                        _Logger.DebugFormat("Error Updating Order record {0} in region {1}: {2}", record.OrderIdentifier, record.RegionIdentifier, errorUpdatingOrderStatusMessage);
        //                    }

        //                }
        //                else if (record.RegionIdentifier == null || record.RegionIdentifier.Length == 0)
        //                {
        //                    string errorUpdatingOrderStatusMessage = string.Empty;
        //                    bool errorUpdatingOrderStatus = false;
        //                    _Logger.DebugFormat("Order record {0} in region {1} has a blank Region Field. Error will be added to Order Record", record.OrderIdentifier, record.RegionIdentifier);
        //                    DBAccessor.UpdateOrderStatus(record.RegionIdentifier, record.OrderIdentifier, "Order has blank order Region field",
        //                        "Error", out errorUpdatingOrderStatusMessage, out errorUpdatingOrderStatus);
        //                    if (!errorUpdatingOrderStatus)
        //                    {
        //                        _Logger.DebugFormat("Order record {0} in region {1} updated successfully", record.OrderIdentifier, record.RegionIdentifier);
        //                    }
        //                    else
        //                    {
        //                        _Logger.DebugFormat("Error Updating Order record {0} in region {1}: {2}", record.OrderIdentifier, record.RegionIdentifier, errorUpdatingOrderStatusMessage);
        //                    }

        //                }
        //                else if (record.ServiceLocationIdentifier == null || record.ServiceLocationIdentifier.Length == 0)
        //                {
        //                    string errorUpdatingOrderStatusMessage = string.Empty;
        //                    bool errorUpdatingOrderStatus = false;
        //                    _Logger.DebugFormat("Order record {0} in region {1} has a blank Service Location field. Error will be added to Order Record", record.OrderIdentifier, record.RegionIdentifier);
        //                    DBAccessor.UpdateOrderStatus(record.RegionIdentifier, record.OrderIdentifier, "Order has blank Service Location field",
        //                        "Error", out errorUpdatingOrderStatusMessage, out errorUpdatingOrderStatus);
        //                    if (!errorUpdatingOrderStatus)
        //                    {
        //                        _Logger.DebugFormat("Order record {0} in region {1} updated successfully", record.OrderIdentifier, record.RegionIdentifier);
        //                    }
        //                    else
        //                    {
        //                        _Logger.DebugFormat("Error Updating Order record {0} in region {1}: {2}", record.OrderIdentifier, record.RegionIdentifier, errorUpdatingOrderStatusMessage);
        //                    }
        //                }
        //                else
        //                {
        //                    noneblankFieldRecords.Add(record);
        //                }
        //            }
        //            //Retrieve Orders with status of New
        //            checkedOrderRecordList = noneblankFieldRecords.FindAll(x => x.Status.ToUpper().Equals("NEW"));

        //            //Retrieve Orders with Delete bit set


        //            List<Order> deleteOrderList = new List<Order>();
        //            string[] originDepotIdentifiers = checkedOrderRecordList.Select(x => x.OriginDepotIdentifier).ToArray();




        //            //Add OrderClass, OrginDepot, and region Entity Keys to Orders if New Orders Exist
        //            if (checkedOrderRecordList.Count != 0)
        //            {
        //                foreach (DBAccess.Records.StagedOrderRecord order in checkedOrderRecordList)
        //                {
        //                    long orderClassEntity = 0;
        //                    long originDepotKey = 0;
        //                    long[] regionEntityKey = new long[] { 0 };
        //                    var temp = (Order)order;

        //                    //Add SerivceTimeType, region and timewindowType entity keys

        //                    if (!orderClassTypes.TryGetValue(order.OrderClassIdentifier, out orderClassEntity))
        //                    {
        //                        _Logger.ErrorFormat("No match found for Order Class with identifier {0} in RNA", order.OrderClassIdentifier);
        //                        temp.OrderClassEntityKey = 0;
        //                    }
        //                    else
        //                    {
        //                        temp.OrderClassEntityKey = orderClassEntity;
        //                    }
        //                    if (!orginDepotTypes.TryGetValue(order.OriginDepotIdentifier, out originDepotKey))
        //                    {
        //                        _Logger.ErrorFormat("No match found for Origin Depot with identifier {0} in RNA", order.OriginDepotIdentifier);
        //                        temp.RequiredRouteOriginEntityKey = 0;
        //                    }
        //                    else
        //                    {
        //                        temp.RequiredRouteOriginEntityKey = originDepotKey;
        //                    }

        //                    if (!regionEntityKeyDic.TryGetValue(order.RegionIdentifier, out regionEntityKey[0]))
        //                    {
        //                        _Logger.ErrorFormat("No match found for Region Entity Key with identifier {0} in RNA", order.RegionIdentifier);
        //                        temp.RegionEntityKey = 0;
        //                    }
        //                    else
        //                    {
        //                        temp.RegionEntityKey = new long();
        //                        temp.RegionEntityKey = regionEntityKey[0];
        //                    }


        //                    checkedOrdersList.Add(temp);
        //                }
        //            }

        //            //Delete orders if Delete orders exist
        //            if (deleteOrderRecordList.Count != 0)
        //            {
        //                foreach (DBAccess.Records.StagedOrderRecord order in deleteOrderRecordList)
        //                {
        //                    if (order.Status.ToUpper().Contains("NEW"))
        //                    {
        //                        var temp = (Order)order;
        //                        temp.Action = ActionType.Delete;
        //                        deleteOrderList.Add(temp);
        //                    }
        //                }

        //                //Unassign and Delete Orders
        //                if (deleteOrderList.Any())
        //                {
        //                    _Logger.DebugFormat("Start Unassigning and Deleting Orders");
        //                    try
        //                    {
        //                        errorLevel = ErrorLevel.None;
        //                        _Logger.DebugFormat(" Start Unassigning Orders");
        //                        string[] unassignOrderIdentifiers = deleteOrderList.Select(x => x.Identifier).ToArray();
        //                        Order[] unassignOrdersInRna = new Order[] { };

        //                        ManipulationResult unassignOrders = new ManipulationResult();
        //                        string regularErrorMessage = string.Empty;
        //                        try
        //                        {
        //                            unassignOrdersInRna = GetOrdersToUnassignOrDeleteInRNA(out errorLevel, out fatalErrorMessage, out regularErrorMessage, unassignOrderIdentifiers);


        //                            if (errorLevel == ErrorLevel.None) //Getting list of orders to unassign that are in RNA Successfull
        //                            {
        //                                try
        //                                {
        //                                    //Unassign Orders
        //                                    Order[] ordersToUnassign = unassignOrdersInRna.Where(x => !x.UnassignedOrderGroupEntityKey.HasValue).ToArray();

        //                                    if (ordersToUnassign.Any())
        //                                    {
        //                                        unassignOrders = UnassignOrders2(out errorLevel, out fatalErrorMessage, ordersToUnassign.ToArray());

        //                                        if (errorLevel == ErrorLevel.None)
        //                                        {
        //                                            _Logger.DebugFormat("Orders Unassign in RNA Successfully");
        //                                        }
        //                                        else if (errorLevel == ErrorLevel.Fatal)
        //                                        {
        //                                            _Logger.Error(fatalErrorMessage);
        //                                        }

        //                                    }
        //                                    else
        //                                    {
        //                                        _Logger.DebugFormat("No Orders to Unassign in RNA");
        //                                    }


        //                                }
        //                                catch (Exception ex)
        //                                {
        //                                    _Logger.Error("An error has occured Unassigning Orders" + ex.Message);
        //                                }

        //                            }
        //                            else if (errorLevel != ErrorLevel.Fatal)
        //                            {
        //                                _Logger.Error(regularErrorMessage);
        //                            }
        //                            else
        //                            {
        //                                _Logger.Error(fatalErrorMessage);
        //                            }

        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            _Logger.Error("An error has occured during Retrieve Delete Orders Entity Keys" + ex.Message);
        //                        }



        //                        if (errorLevel != ErrorLevel.Fatal)
        //                        {
        //                            _Logger.DebugFormat("Unassigning Orders Successful.");

        //                            //Get list of Delete Orders in RNA with Entity Keys
        //                            string[] orderToDeleteIdentifiers = deleteOrderList.Select(x => x.Identifier).ToArray();
        //                            try
        //                            {
        //                                Order[] orderToDelWithEntityKeys = GetOrdersToUnassignOrDeleteInRNA(out errorLevel, out fatalErrorMessage, out regularErrorMessage, orderToDeleteIdentifiers);

        //                                foreach (Order order in orderToDelWithEntityKeys)
        //                                {
        //                                    order.Action = ActionType.Delete;
        //                                }

        //                                if (errorLevel == ErrorLevel.None)
        //                                {
        //                                    _Logger.DebugFormat(" Start Deleting Orders");
        //                                    SaveResult[] deleteOrdersResult = DeleteOrders(out errorLevel, out fatalErrorMessage, orderToDelWithEntityKeys);

        //                                    if (errorLevel == ErrorLevel.None)
        //                                    {
        //                                        _Logger.DebugFormat("Deleting Orders Successful");
        //                                        foreach (String orderId in orderToDeleteIdentifiers)
        //                                        {
        //                                            string databaseErrorMessage = string.Empty;
        //                                            bool datbaseErrorCaught = false;
        //                                            DBAccessor.UpdateOrderStatus(_Region.Identifier, orderId, "", "COMPLETE", out databaseErrorMessage, out datbaseErrorCaught);

        //                                            if (datbaseErrorCaught == false)
        //                                            {
        //                                                _Logger.DebugFormat("Update Staged Orders Table for order " + orderId);
        //                                            }
        //                                            else
        //                                            {

        //                                                _Logger.ErrorFormat("Error Updating Staged Order table for order {0} | {1}: {2} ", orderId, databaseErrorMessage);


        //                                            }
        //                                        }
        //                                    }
        //                                    else
        //                                    {
        //                                        foreach (SaveResult saveResult in deleteOrdersResult)
        //                                        {
        //                                            var temp = (Order)saveResult.Object;
        //                                            _Logger.ErrorFormat("Error Deleting Order {0} | {1}: {2} ", temp.Identifier, saveResult.Error.Code, saveResult.Error.Detail.ToString());
        //                                        }

        //                                    }
        //                                }
        //                                else if (errorLevel == ErrorLevel.Transient)
        //                                {
        //                                    _Logger.DebugFormat(" No Orders to Delete from RNA. Orders do not Exist in RNA");
        //                                    foreach (String orderId in orderToDeleteIdentifiers)
        //                                    {
        //                                        string databaseErrorMessage = string.Empty;
        //                                        bool datbaseErrorCaught = false;
        //                                        DBAccessor.UpdateOrderStatus(_Region.Identifier, orderId, "", "COMPLETE", out databaseErrorMessage, out datbaseErrorCaught);

        //                                        if (datbaseErrorCaught == false)
        //                                        {
        //                                            _Logger.DebugFormat("Updated Staged Orders Table for order " + orderId);
        //                                        }
        //                                        else
        //                                        {

        //                                            _Logger.ErrorFormat("Error Updating Staged Order table for order {0} | {1}: {2} ", orderId, databaseErrorMessage);


        //                                        }
        //                                    }
        //                                }
        //                                else
        //                                {
        //                                    _Logger.Error(fatalErrorMessage);
        //                                    foreach (String orderId in orderToDeleteIdentifiers)
        //                                    {
        //                                        string databaseErrorMessage = string.Empty;
        //                                        bool datbaseErrorCaught = false;
        //                                        DBAccessor.UpdateOrderStatus(_Region.Identifier, orderId, fatalErrorMessage, "ERROR", out databaseErrorMessage, out datbaseErrorCaught);

        //                                        if (datbaseErrorCaught == false)
        //                                        {
        //                                            _Logger.DebugFormat("Updated Staged Orders Table for order " + orderId);
        //                                        }
        //                                        else
        //                                        {

        //                                            _Logger.ErrorFormat("Error Updating Staged Order table for order {0} | {1}: {2} ", orderId, databaseErrorMessage);


        //                                        }
        //                                    }
        //                                }
        //                            }
        //                            catch (Exception ex)
        //                            {
        //                                _Logger.Error("An error has occured during delete Orders" + ex.Message);
        //                            }
        //                        }
        //                        else
        //                        {
        //                            _Logger.ErrorFormat("Fatal Error Unassigning Orders: " + fatalErrorMessage);
        //                        }

        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        _Logger.Error("An error has occured during Unassigning and Deleting Orders" + ex.Message);
        //                    }
        //                }
        //            }


        //            try
        //            {
        //                List<string> sessionsToCreate = new List<string>();
        //                errorLevel = ErrorLevel.None;

        //                //Retrieve Tomorrows Routing Session and Create Routing Sessions

        //                _Logger.Debug("Start Retrieving DailyRoutingSessions");
        //                DailyRoutingSession[] tomorrowsRoutingSession = RetrieveDailyRoutingSessionwithOrigin(out errorLevel, out fatalErrorMessage, DateTime.Now.AddDays(1), originDepotIdentifiers);

        //                if (errorLevel == ErrorLevel.None)
        //                {
        //                    _Logger.Debug("Retreived DailyRoutingSessions Successfully");
        //                    //Create list of daily routing session for tomorrow
        //                    foreach (DailyRoutingSession session in tomorrowsRoutingSession)
        //                    {
        //                        saveRoutingSession.Add(session);
        //                    }

        //                    //List depots that don't have routing session
        //                    foreach (String origindepot in originDepotIdentifiers)
        //                    {
        //                        if (!tomorrowsRoutingSession.Any(x => x.Description == origindepot))
        //                        {
        //                            sessionsToCreate.Add(origindepot);
        //                        }

        //                    }
        //                }
        //                else if (errorLevel == ErrorLevel.Fatal)
        //                {
        //                    _Logger.Error("An error has occured during Retrieving DailyRoutingSession " + fatalErrorMessage);
        //                }

        //                //create routing sessions for depots that don't have routing sessions

        //                if (sessionsToCreate.Any())
        //                {
        //                    try
        //                    {
        //                        _Logger.DebugFormat(" Start Creating Daily Routing Sessions for Depots");
        //                        SaveResult[] newRoutingSessions = SaveDailyRoutingSessions(out errorLevel, out fatalErrorMessage, new DateTime[] { DateTime.Now.AddDays(1) }, sessionsToCreate.ToArray());

        //                        if (errorLevel == ErrorLevel.None)
        //                        {

        //                            foreach (SaveResult saveResult in newRoutingSessions)
        //                            {


        //                                if (saveResult.Error != null)
        //                                {
        //                                    var temp = (DailyRoutingSession)saveResult.Object;
        //                                    _Logger.Error("An error has occured while creating session for Depot " + temp.Description + ":" + saveResult.Error.Code + " " + saveResult.Error.Detail);
        //                                }
        //                                else
        //                                {
        //                                    var temp = (DailyRoutingSession)saveResult.Object;
        //                                    _Logger.Debug("Created Session for session for Depot " + temp.Description);
        //                                    saveRoutingSession.Add(temp);

        //                                }
        //                            }
        //                            _Logger.DebugFormat(" Creating Daily Routing Sessions for Depots Ended");
        //                        }
        //                        else if (errorLevel == ErrorLevel.Fatal)
        //                        {
        //                            _Logger.Error("A Fatal Error has occured while creating session" + fatalErrorMessage);
        //                        }



        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        _Logger.Error("An error has occured during Creating Routing Sessions" + ex.Message);
        //                    }
        //                }



        //            }
        //            catch (Exception ex)
        //            {
        //                _Logger.Error("An error has occured during Retrieving DailyRoutingSessions" + ex.Message);
        //            }

        //            try
        //            {
        //                _Logger.Debug("Retreiving Matching Orders In RNA Since " + RegionProcessor.lastSuccessfulRunTime.ToLongDateString());

        //                //Retrive Modified and Matching Orders

        //                string[] orderIDDatabase = checkedOrderRecordList.Select(x => x.OrderIdentifier).ToArray();

        //                rnaOrders = RetrieveOrdersFromRNA(out errorLevel, out fatalErrorMessage, orderIDDatabase);

        //                if (errorLevel == ErrorLevel.None && !rnaOrders.Any())
        //                {
        //                    _Logger.DebugFormat("No Orders Modified Since last execution time");
        //                }
        //                else if (errorLevel == ErrorLevel.Fatal)
        //                {
        //                    _Logger.Error(fatalErrorMessage);
        //                }


        //            }
        //            catch (Exception ex)
        //            {
        //                _Logger.Error("An error has occured retrieving modified orders" + ex.Message);
        //            }

        //            List<OrderSpec> convertedOrderSpecs = new List<OrderSpec>();




        //            if (rnaOrders.Count == 0 || rnaOrders == null) //Orders have not been found, Add Orders to RNA
        //            {



        //                List<ServiceLocation> serviceLocationsforOrdersInRegion = new List<ServiceLocation>();



        //                try
        //                {

        //                    try//Get Region Service Locations
        //                    {
        //                        long[] regionEntityKey = new long[] { _Region.EntityKey };
        //                        _Logger.Debug("Start Retrieved Service Locations for Orders");
        //                        serviceLocationsforOrdersInRegion = RetrieveServiceLocationsByRegion(out errorLevel, out fatalErrorMessage, regionEntityKey).ToList();
        //                        if (errorLevel == ErrorLevel.None)
        //                        {
        //                            _Logger.Debug("Successfully Retrieved Service Locations");

        //                        }
        //                        else if (errorLevel == ErrorLevel.Fatal)
        //                        {
        //                            _Logger.Error("Fatal Error Retrieving Service Locations for Orders" + fatalErrorMessage);
        //                        }
        //                        else
        //                        {
        //                            _Logger.Error("Error Retrieving Service Locations for Orders");
        //                        }

        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        _Logger.Error("An error has occured retrieving service locations for orders during saving Orders" + ex.Message);
        //                    }

        //                    List<Order> ordersToSaveInRNA = new List<Order>();

        //                    if (checkedOrderRecordList.Count != 0)
        //                    {
        //                        foreach (DBAccess.Records.StagedOrderRecord order in checkedOrderRecordList) //Find Order with Matching service Location and convert them to Order Spec
        //                        {
        //                            ServiceLocation tempLocation = serviceLocationsforOrdersInRegion.FirstOrDefault(x => x.Identifier.ToUpper() == order.ServiceLocationIdentifier.ToUpper());
        //                            Order tempOrder = new Order();
        //                            Task orderTask = new Task();
        //                            List<TaskServiceWindowOverrideDetail> serviceWindowOverides = new List<TaskServiceWindowOverrideDetail>();
        //                            List<TaskOpenCloseOverrideDetail> openCloseOverride = new List<TaskOpenCloseOverrideDetail>();
        //                            if (order.ServiceWindowOverride1End.Any() && order.ServiceWindowOverride1Start.Any())
        //                            {
        //                                var temp = new TaskServiceWindowOverrideDetail
        //                                {
        //                                    Action = ActionType.Add,
        //                                    DailyTimePeriod = new DailyTimePeriod
        //                                    {
        //                                        DayOfWeekFlags_DaysOfWeek = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
        //                                        StartTime = order.ServiceWindowOverride1Start,
        //                                        EndTime = order.ServiceWindowOverride1End,



        //                                    }

        //                                };
        //                                var temp2 = new TaskOpenCloseOverrideDetail
        //                                {
        //                                    Action = ActionType.Add,
        //                                    DailyTimePeriod = new DailyTimePeriod
        //                                    {
        //                                        DayOfWeekFlags_DaysOfWeek = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
        //                                        StartTime = order.ServiceWindowOverride1Start,
        //                                        EndTime = order.ServiceWindowOverride1End

        //                                    }
        //                                };


        //                                serviceWindowOverides.Add(temp);
        //                                openCloseOverride.Add(temp2);
        //                            }
        //                            if (order.ServiceWindowOverride2End.Any() && order.ServiceWindowOverride2Start.Any())
        //                            {
        //                                var temp = new TaskServiceWindowOverrideDetail
        //                                {

        //                                    Action = ActionType.Add,
        //                                    DailyTimePeriod = new DailyTimePeriod
        //                                    {
        //                                        DayOfWeekFlags_DaysOfWeek = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
        //                                        StartTime = order.ServiceWindowOverride2Start,
        //                                        EndTime = order.ServiceWindowOverride2End,

        //                                    },


        //                                };
        //                                var temp2 = new TaskOpenCloseOverrideDetail
        //                                {
        //                                    Action = ActionType.Add,
        //                                    DailyTimePeriod = new DailyTimePeriod
        //                                    {
        //                                        DayOfWeekFlags_DaysOfWeek = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
        //                                        StartTime = order.ServiceWindowOverride2Start,
        //                                        EndTime = order.ServiceWindowOverride2End,

        //                                    }
        //                                };


        //                                serviceWindowOverides.Add(temp);
        //                                openCloseOverride.Add(temp2);
        //                            }
        //                            if (tempLocation == null) // order service location found in RNA service locations
        //                            {
        //                                _Logger.DebugFormat(" Service Location {1} for Order {0} not Found in RNA ", order.OrderIdentifier, order.ServiceLocationIdentifier);


        //                            }
        //                            else
        //                            {
        //                                orderTask = new Task
        //                                {
        //                                    Action = ActionType.Add,
        //                                    LocationAddress = tempLocation.Address,
        //                                    LocationPhoneNumber = tempLocation.PhoneNumber,
        //                                    LocationEntityKey = tempLocation.EntityKey,
        //                                    LocationCoordinate = tempLocation.Coordinate,
        //                                    LocationDescription = tempLocation.Description,
        //                                    LocationStandardInstructions = tempLocation.StandardInstructions,
        //                                    ServiceWindowOverrides = serviceWindowOverides.ToArray(),
        //                                    TaskType_Type = "Delivery",
        //                                    OpenCloseOverrides = openCloseOverride.ToArray()



        //                                };



        //                                tempOrder = (Order)order;
        //                                tempOrder.PreferredRouteIdentifierOverride = tempOrder.PreferredRouteIdentifier;
        //                                tempOrder.Action = ActionType.Add;
        //                                tempOrder.Tasks = new Task[] { orderTask };
        //                                long orderClassEntityKey = 0;
        //                                long origionDepotEntityKey = 0;

        //                                if (orderClassTypes.TryGetValue(order.OrderClassIdentifier, out orderClassEntityKey))
        //                                {
        //                                    tempOrder.OrderClassEntityKey = Convert.ToInt64(orderClassEntityKey);
        //                                }
        //                                else
        //                                {
        //                                    _Logger.Debug("Error Getting Entity Key For Order Class " + order.OrderClassIdentifier);
        //                                }



        //                                if (orginDepotTypes.TryGetValue(order.OriginDepotIdentifier, out origionDepotEntityKey))
        //                                {
        //                                    tempOrder.RequiredRouteOriginEntityKey = Convert.ToInt64(origionDepotEntityKey);
        //                                }
        //                                else
        //                                {
        //                                    _Logger.Debug("Error Getting Entity Key For Origin Depot " + order.OriginDepotIdentifier);
        //                                }

        //                                //Add session entity key to order for routings session with matching depots
        //                                foreach (DailyRoutingSession session in saveRoutingSession)
        //                                {
        //                                    if (order.OriginDepotIdentifier == session.Description)
        //                                    {
        //                                        tempOrder.SessionEntityKey = session.EntityKey;
        //                                        // tempOrder.BeginDate = session.StartDate.ToString();
        //                                    }
        //                                }


        //                                ordersToSaveInRNA.Add(tempOrder);

        //                            }
        //                        }


        //                        foreach (Order orderSave in ordersToSaveInRNA)
        //                        {
        //                            if (orderSave.Action == ActionType.Update)
        //                            {
        //                                convertedOrderSpecs.Add(ConvertOrderToOrderSpec(orderSave));
        //                            }
        //                            else if (orderSave.Action == ActionType.Add)
        //                            {
        //                                var temp = ConvertOrderToOrderSpec(orderSave);
        //                                temp.OrderInstance = null;
        //                                convertedOrderSpecs.Add(temp);
        //                            }

        //                        }

        //                    }
        //                }
        //                catch (Exception ex)
        //                {
        //                    _Logger.Error("An error has occured during saving Orders" + ex.Message);
        //                }
        //            }

        //            else  //Orders have a been found in RNA
        //            {
        //                // Order Checked List and Returned RNA List
        //                checkedOrdersList.OrderBy(x => x.Identifier); // New Orders
        //                rnaOrders.OrderBy(x => x.Identifier); //Orders in RNA
        //                long[] regionEntityKey = new long[] { _Region.EntityKey };
        //                List<Order> updateOrders = new List<Order>(); //Orders to Update in RNA
        //                List<Order> regOrders = new List<Order>(); //Orders to Add to RNA
        //                List<Order> saveOrdersList = new List<Order>();
        //                List<ServiceLocation> serviceLocationsforOrdersInRegion = RetrieveServiceLocationsByRegion(out errorLevel, out fatalErrorMessage, regionEntityKey).ToList();

        //                List<Order> ordersInRnaOrders = checkedOrdersList.Where(x => rnaOrders.Any(y => y.Identifier == x.Identifier)).ToList();
        //                List<Order> ordersNotInRnaOrders = checkedOrdersList.Where(x => !rnaOrders.Any(y => y.Identifier == x.Identifier)).ToList();


        //                foreach (Order orderInRNA in ordersInRnaOrders)// found matching order in RNA
        //                {
        //                    Order tempOrder = rnaOrders.FirstOrDefault(x => x.Identifier == orderInRNA.Identifier);
        //                    orderInRNA.Action = ActionType.Update;
        //                    orderInRNA.Tasks[0].TaskType_Type = "Delivery";
        //                    orderInRNA.EntityKey = tempOrder.EntityKey;
        //                    orderInRNA.Version = tempOrder.Version;
        //                    orderInRNA.PreferredRouteIdentifierOverride = orderInRNA.PreferredRouteIdentifier;
        //                    orderInRNA.SessionDate = tempOrder.SessionDate;
        //                    orderInRNA.SessionEntityKey = tempOrder.SessionEntityKey;
        //                    orderInRNA.SessionDescription = tempOrder.SessionDescription;
        //                    orderInRNA.SessionMode_SessionMode = tempOrder.SessionMode_SessionMode;

        //                    //Change Service Windows entity Keys
        //                    //if rna order has service windows overrides and database orders have service window overrides



        //                    if ((orderInRNA.Tasks[0].ServiceWindowOverrides.Length == tempOrder.Tasks[0].ServiceWindowOverrides.Length) && orderInRNA.Tasks[0].ServiceWindowOverrides != null && orderInRNA.Tasks[0].ServiceWindowOverrides.Length != 0)
        //                    {
        //                        for (int i = 0; i < orderInRNA.Tasks[0].ServiceWindowOverrides.Length; i++)
        //                        {
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[i].EntityKey = tempOrder.Tasks[0].ServiceWindowOverrides[i].EntityKey;
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[i].Action = ActionType.Update;
        //                        }
        //                    }
        //                    else if ((tempOrder.Tasks[0].ServiceWindowOverrides == null || tempOrder.Tasks[0].ServiceWindowOverrides.Length == 0) && orderInRNA.Tasks[0].ServiceWindowOverrides.Length != 0)
        //                    {
        //                        for (int i = 0; i < orderInRNA.Tasks[0].ServiceWindowOverrides.Length; i++)
        //                        {
        //                            if (orderInRNA.Tasks[0].ServiceWindowOverrides[i].EntityKey == 0)
        //                            {
        //                                orderInRNA.Tasks[0].ServiceWindowOverrides[i].Action = ActionType.Add;
        //                            }
        //                            else
        //                            {
        //                                orderInRNA.Tasks[0].ServiceWindowOverrides[i].Action = ActionType.Update;
        //                                orderInRNA.Tasks[0].ServiceWindowOverrides[i].EntityKey = tempOrder.Tasks[0].ServiceWindowOverrides[i].EntityKey;
        //                            }


        //                        }
        //                    }
        //                    else
        //                    {

        //                        if (orderInRNA.Tasks[0].ServiceWindowOverrides.Length == 1 && tempOrder.Tasks[0].ServiceWindowOverrides.Length == 2)
        //                        {

        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[0].EntityKey = tempOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[1].EntityKey = tempOrder.Tasks[0].ServiceWindowOverrides[1].EntityKey;
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[1].Action = ActionType.Delete;

        //                        }
        //                        else if (orderInRNA.Tasks[0].ServiceWindowOverrides.Length == 2 && tempOrder.Tasks[0].ServiceWindowOverrides.Length == 1)
        //                        {

        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[0].EntityKey = tempOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;
        //                            orderInRNA.Tasks[0].ServiceWindowOverrides[1].Action = ActionType.Add;

        //                        }


        //                    }


        //                    ServiceLocation tempLocation = serviceLocationsforOrdersInRegion.FirstOrDefault(x => x.Identifier == orderInRNA.Tasks[0].LocationIdentifier); //Find service location that matched order

        //                    if (tempLocation != null) // order service location found in RNA service locations
        //                    {
        //                        orderInRNA.Tasks[0].Action = ActionType.Update;
        //                        orderInRNA.Tasks[0].LocationAddress = tempLocation.Address;
        //                        orderInRNA.Tasks[0].LocationPhoneNumber = tempLocation.PhoneNumber;
        //                        orderInRNA.Tasks[0].LocationEntityKey = tempLocation.EntityKey;
        //                        orderInRNA.Tasks[0].LocationCoordinate = tempLocation.Coordinate;
        //                        orderInRNA.Tasks[0].LocationDescription = tempLocation.Description;
        //                    }

        //                    string originDepotForOrder = orginDepotTypes.FirstOrDefault(x => x.Value == orderInRNA.RequiredRouteOriginEntityKey).Key;
        //                    orderInRNA.RequiredRouteOriginEntityKey = orginDepotTypes.FirstOrDefault(x => x.Value == orderInRNA.RequiredRouteOriginEntityKey).Value;


        //                    //Don't add session entity key for updated orders
        //                    /*foreach (DailyRoutingSession session in saveRoutingSession)
        //                    {
        //                        if (originDepotForOrder == session.Description)
        //                        {
        //                            orderInRNA.SessionEntityKey = session.EntityKey;
        //                            orderInRNA.BeginDate = session.StartDate.ToString();
        //                        }
        //                    }*/
        //                    updateOrders.Add(orderInRNA);
        //                }
        //                foreach (Order orderNotInRNAs in ordersNotInRnaOrders) // No matching Order Found. Add location/coordinate to order and save in regOrder List
        //                {
        //                    orderNotInRNAs.Action = ActionType.Add;


        //                    ServiceLocation tempLocation2 = serviceLocationsforOrdersInRegion.FirstOrDefault(x => x.Identifier == orderNotInRNAs.Tasks[0].LocationIdentifier); //Find service location that matched order

        //                    if (tempLocation2 != null) // order service location found in RNA service locations
        //                    {
        //                        orderNotInRNAs.Tasks[0].Action = ActionType.Add;
        //                        orderNotInRNAs.Tasks[0].LocationAddress = tempLocation2.Address;
        //                        orderNotInRNAs.Tasks[0].LocationPhoneNumber = tempLocation2.PhoneNumber;
        //                        orderNotInRNAs.Tasks[0].LocationEntityKey = tempLocation2.EntityKey;
        //                        orderNotInRNAs.Tasks[0].LocationCoordinate = tempLocation2.Coordinate;
        //                        orderNotInRNAs.Tasks[0].LocationDescription = tempLocation2.Description;
        //                        orderNotInRNAs.Tasks[0].LocationStandardInstructions = tempLocation2.StandardInstructions;
        //                    }

        //                    string originDepotForOrder = orginDepotTypes.FirstOrDefault(x => x.Value == orderNotInRNAs.RequiredRouteOriginEntityKey).Key;
        //                    orderNotInRNAs.RequiredRouteOriginEntityKey = orginDepotTypes.FirstOrDefault(x => x.Value == orderNotInRNAs.RequiredRouteOriginEntityKey).Value;

        //                    //Add session entity key to order for routings session with matching depots
        //                    foreach (DailyRoutingSession session in saveRoutingSession)
        //                    {
        //                        if (originDepotForOrder == session.Description)
        //                        {
        //                            orderNotInRNAs.SessionEntityKey = session.EntityKey;
        //                            //orderNotInRNAs.BeginDate = session.StartDate.ToString();
        //                        }
        //                    }
        //                    regOrders.Add(orderNotInRNAs);
        //                }



        //                //Add Update and Reg orders to a list

        //                saveOrdersList = updateOrders.Concat(regOrders).ToList();

        //                //Convert Order to OrderSpec
        //                foreach (Order order in saveOrdersList)
        //                {
        //                    if (order.Action == ActionType.Update)
        //                    {
        //                        convertedOrderSpecs.Add(ConvertOrderToOrderSpec(order));
        //                    }
        //                    else if (order.Action == ActionType.Add)
        //                    {
        //                        var temp = ConvertOrderToOrderSpec(order);
        //                        temp.OrderInstance = null;
        //                        convertedOrderSpecs.Add(temp);
        //                    }

        //                }
        //            } //matching orders found in rna






        //            //Save Orders to RNA
        //            try
        //            {

        //                foreach (OrderSpec order in convertedOrderSpecs)
        //                {
        //                    order.RegionEntityKey = _Region.EntityKey;
        //                    order.ManagedByUserEntityKey = MainService.User.EntityKey;
        //                    order.Identifier = order.Identifier.ToUpper();

        //                };

        //                if (convertedOrderSpecs.Count != 0)
        //                {
        //                    foreach (OrderSpec orderSpec in convertedOrderSpecs)
        //                    {
        //                        SaveResult[] savedOrdersResult = new SaveResult[] { };
        //                        savedOrdersResult = SaveOrders(out errorLevel, out fatalErrorMessage, new OrderSpec[] { orderSpec });

        //                        if (errorLevel == ApexConsumer.ErrorLevel.Fatal || savedOrdersResult == null)
        //                        {
        //                            _Logger.Debug("Fatel Error Occured Saving Orders : " + fatalErrorMessage);
        //                        }
        //                        else
        //                        {
        //                            string errorUpdatingOrderStatusMessage = string.Empty;
        //                            bool errorUpdatingOrderStatus = false;

        //                            foreach (SaveResult result in savedOrdersResult)
        //                            {
        //                                var tempOrder = (Order)result.Object;
        //                                if (result.Error == null)
        //                                {
        //                                    _Logger.DebugFormat("Successfully Saved Order {0} to RNA", orderSpec.Identifier);
        //                                    DBAccessor.UpdateOrderStatus(_Region.Identifier, orderSpec.Identifier, "Successfully Saved Order", "COMPLETE", out errorUpdatingOrderStatusMessage, out errorUpdatingOrderStatus);
        //                                }
        //                                else if ((result.Error != null || tempOrder != null) && result.Error.ValidationFailures == null)
        //                                {
        //                                    _Logger.ErrorFormat("An error has occured during saving Orders. The Order {0} has the following error {1}: {2}", orderSpec.Identifier, result.Error.Code.ErrorCode_Status, result.Error.Detail);
        //                                    DBAccessor.UpdateOrderStatus(_Region.Identifier, orderSpec.Identifier, "Error Occured: " + result.Error.Code.ErrorCode_Status, "ERROR", out errorUpdatingOrderStatusMessage, out errorUpdatingOrderStatus);
        //                                }
        //                                else if (result.Error.ValidationFailures != null)
        //                                {
        //                                    _Logger.ErrorFormat("An error has occured during saving Orders. The Order {0} property {1} is not valid or in the proper format", orderSpec.Identifier, result.Error.ValidationFailures[0].Property);
        //                                    DBAccessor.UpdateOrderStatus(_Region.Identifier, orderSpec.Identifier, "Validation Error For Properties " + result.Error.ValidationFailures[0].Property + "See Log", "ERROR", out errorUpdatingOrderStatusMessage, out errorUpdatingOrderStatus);
        //                                }

        //                                _Logger.DebugFormat("Saving Order {0} Completed", tempOrder.Identifier);
        //                            }

        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    _Logger.Debug("No Orders to Save to RNA");
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                _Logger.Error(ex.Message);
        //                errorRetrieveAndSavingOrdersFromStagingTable = true;
        //                errorRetrieveAndSavingOrdersFromStagingTableMessage = ex.Message;
        //            }
        //        }
        //    }

        //    catch (FaultException<TransferErrorCode> tec)
        //    {
        //        string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
        //        _Logger.Error("Retrieve Service Location | " + errorMessage);

        //        if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
        //        {
        //            _Logger.Info("Session has expired. New session required.");
        //            MainService.SessionRequired = true;
        //            errorLevel = ErrorLevel.Transient;
        //        }
        //        else
        //        {
        //            errorLevel = ErrorLevel.Fatal;
        //            fatalErrorMessage = errorMessage;
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        _Logger.Error(ex.Message);
        //        errorLevel = ErrorLevel.Transient;
        //    }




        //}

        //public void RetrieveDummyOrdersAndSave(Dictionary<string, long> RetrieveDepotsForRegionDict, Dictionary<string, long> RetrieveOrderClassDict,
        //    string regionId, out bool errorRetrieveOrdersFromCSV, out string errorRetrieveOrdersFromCSVMessage)
        //{

        //    errorRetrieveOrdersFromCSV = false;
        //    errorRetrieveOrdersFromCSVMessage = string.Empty;
        //    List<DBAccess.Records.StagedOrderRecord> retrieveListOrders = new List<DBAccess.Records.StagedOrderRecord>();
        //    DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);
        //    List<Order> dummyOrdersFromCSV = new List<Order>();
        //    bool errorCaught = false; ;
        //    List<DailyRoutingSession> saveRoutingSession = new List<DailyRoutingSession>();
        //    string fatalErrorMessage = string.Empty;
        //    List<Order> rnaOrders = new List<Order>();
        //    string[] dummyOrderIDs = new string[] { };
        //    List<Order> modifiedRnaOrders = new List<Order>();





        //    try
        //    {
        //        //Get Dummy Orders
        //        _Logger.Debug("Start Retrieving Dummy Orders");
        //        dummyOrdersFromCSV = RetrieveDummyOrdersFromCSV(RetrieveDepotsForRegionDict, RetrieveOrderClassDict, regionId, out errorRetrieveOrdersFromCSV, out errorRetrieveOrdersFromCSVMessage);



        //        if (errorRetrieveOrdersFromCSV)
        //        {
        //            _Logger.Error(errorRetrieveOrdersFromCSVMessage);

        //        }
        //        else if (dummyOrdersFromCSV == null || dummyOrdersFromCSV.Count == 0)
        //        {
        //            _Logger.Debug("Dummy Orders Retrieved Successfully");
        //            _Logger.Debug("No Dummy Orders Found in Dummy Order File");
        //        }
        //        else
        //        {
        //            _Logger.Debug("Dummy Orders Retrieved Successfully");
        //            _Logger.DebugFormat("{0} Dummy Orders Found in Dummy Order File", dummyOrdersFromCSV.Count());
        //            dummyOrderIDs = dummyOrdersFromCSV.Select(x => x.Identifier).ToArray();

        //            //Get Origin Depot Keys
        //            string originDepotIdentifier = dummyOrdersFromCSV.Select(x => RetrieveDepotsForRegionDict.FirstOrDefault(y => y.Value == x.RequiredRouteOriginEntityKey).Key).First();

        //            List<string> sessionsToCreate = new List<string>();
        //            errorCaught = false;

        //            //Retrieve Tomorrows Routing Session and Create Routing Sessions

        //            _Logger.Debug("Start Retrieving DailyRoutingSessions");
        //            DailyRoutingSession[] tomorrowsRoutingSession = RetrieveDailyRoutingSessionwithOrigin(out errorCaught, out fatalErrorMessage, DateTime.Now, originDepotIdentifier);

        //            if (!errorCaught)
        //            {
        //                _Logger.Debug("Retreived DailyRoutingSessions Successfully");
        //                //Create list of daily routing session for tomorrow
        //                foreach (DailyRoutingSession session in tomorrowsRoutingSession)
        //                {
        //                    saveRoutingSession.Add(session);
        //                }

        //                //List depots that don't have routing session

        //                if (!tomorrowsRoutingSession.Any(x => x.Description == originDepotIdentifier))
        //                {
        //                    sessionsToCreate.Add(originDepotIdentifier);
        //                }


        //            }
        //            else if (errorCaught)
        //            {
        //                _Logger.Error("An error has occured during Retrieving DailyRoutingSession " + fatalErrorMessage);
        //            }

        //            //create routing sessions for depots that don't have routing sessions

        //            if (sessionsToCreate.Any())
        //            {
        //                try
        //                {
        //                    _Logger.DebugFormat(" Start Creating Daily Routing Sessions for Depots");
        //                    SaveResult[] newRoutingSessions = SaveDailyRoutingSessions(out errorCaught, out fatalErrorMessage, new DateTime[] { DateTime.Now.AddDays(1) }, sessionsToCreate.ToArray());

        //                    if (!errorCaught)
        //                    {

        //                        foreach (SaveResult saveResult in newRoutingSessions)
        //                        {


        //                            if (saveResult.Error != null)
        //                            {
        //                                var temp = (DailyRoutingSession)saveResult.Object;
        //                                _Logger.Error("An error has occured while creating session for Depot " + temp.Description + ":" + saveResult.Error.Code + " " + saveResult.Error.Detail);
        //                            }
        //                            else
        //                            {
        //                                var temp = (DailyRoutingSession)saveResult.Object;
        //                                _Logger.Debug("Created Session for session for Depot " + temp.Description);
        //                                saveRoutingSession.Add(temp);

        //                            }
        //                        }
        //                        _Logger.DebugFormat(" Creating Daily Routing Sessions for Depots Ended");
        //                    }
        //                    else if (!errorCaught)
        //                    {
        //                        _Logger.Error("A Fatal Error has occured while creating session" + fatalErrorMessage);
        //                    }



        //                }
        //                catch (Exception ex)
        //                {
        //                    _Logger.Error("An error has occured during Creating Routing Sessions" + ex.Message);
        //                }
        //            }

        //            //Get matching Orders from RNA

        //            try
        //            {
        //                _Logger.Debug("Retrieve Matching Pick Up Dummy Orders created in RNA Since " + RegionProcessor.lastSuccessfulRunTime.ToString());

        //                foreach (String orderNumber in dummyOrderIDs)
        //                {
        //                    ErrorLevel errorLevel = ErrorLevel.None;

        //                    Order tempOrder = RetrieveDummyRNAOrders(out errorLevel, out fatalErrorMessage, orderNumber);


        //                    if (errorLevel == ErrorLevel.None && tempOrder == null)
        //                    {

        //                        _Logger.DebugFormat("No Dummy Orders in RNA matching Order Number " + orderNumber);
        //                    }
        //                    else if (errorLevel == ErrorLevel.Fatal)
        //                    {
        //                        _Logger.Error(fatalErrorMessage);

        //                    }
        //                    else if (errorLevel == ErrorLevel.None && tempOrder != null)
        //                    {
        //                        modifiedRnaOrders.Add(tempOrder);
        //                        _Logger.Debug("Sucessfully found and Retrieved Dummy Order" + orderNumber + " in RNA");

        //                    }
        //                }

        //                _Logger.Debug("Sucessfully Retrieved " + modifiedRnaOrders.Count.ToString() + " dummy orders from RNA");

        //            }
        //            catch (Exception ex)
        //            {
        //                _Logger.Error("An error has occured retrieving modified dummy orders" + ex.Message);
        //            }

        //            List<OrderSpec> convertedOrderSpecs = new List<OrderSpec>();

        //            //Check if Matching orders Exist in RNA

        //            rnaOrders = modifiedRnaOrders;


        //            if (rnaOrders.Count == 0 || rnaOrders == null) //Orders have not been found that match dummy pickup orders, Add Orders to RNA
        //            {
        //                List<ServiceLocation> serviceLocationsforOrdersInRegion = new List<ServiceLocation>();

        //                try
        //                {

        //                    try//Get Region Service Locations
        //                    {
        //                        ErrorLevel errorLevel = ErrorLevel.None;
        //                        long[] regionEntityKey = new long[] { _Region.EntityKey };
        //                        _Logger.Debug("Start Retrieved Service Locations for Dummy Orders");
        //                        serviceLocationsforOrdersInRegion = RetrieveServiceLocationsByRegion(out errorLevel, out fatalErrorMessage, regionEntityKey).ToList();
        //                        if (errorLevel == ErrorLevel.None)
        //                        {
        //                            _Logger.Debug("Successfully Retrieved Service Locations for Dummy Orders");

        //                        }
        //                        else if (errorLevel == ErrorLevel.Fatal)
        //                        {
        //                            _Logger.Error("Fatal Error Retrieving Service Locations for Dummy Orders:" + fatalErrorMessage);
        //                        }
        //                        else
        //                        {
        //                            _Logger.Error("Error Retrieving Service Locations for Dummy Orders");
        //                        }

        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        _Logger.Error("An error has occured retrieving service locations for orders during saving Orders" + ex.Message);
        //                    }

        //                    List<Order> ordersToSaveInRNA = new List<Order>();
        //                    foreach (Order dummyorder in dummyOrdersFromCSV) //Find Order with Matching service Location and convert them to Order Spec
        //                    {
        //                        Task orderTask = new Task();
        //                        ServiceLocation tempLocation = serviceLocationsforOrdersInRegion.FirstOrDefault(x => x.Identifier.ToUpper() == dummyorder.Tasks[0].LocationIdentifier.ToUpper());

        //                        if (tempLocation == null) // order service location found in RNA service locations
        //                        {
        //                            _Logger.DebugFormat("Could not find service location {0} for Dummy Order {1} in RNA", dummyorder.Tasks[0].LocationIdentifier, dummyorder.Identifier);
        //                        }
        //                        else
        //                        {
        //                            dummyorder.Tasks[0].LocationEntityKey = tempLocation.EntityKey;
        //                            dummyorder.Tasks[0].LocationIdentifier = tempLocation.Identifier;
        //                            dummyorder.Tasks[0].LocationAddress = tempLocation.Address;
        //                            dummyorder.Tasks[0].LocationCoordinate = tempLocation.Coordinate;
        //                            dummyorder.Tasks[0].TaskType_Type = "Pickup";

        //                            string originDepotForOrder = RetrieveDepotsForRegionDict.FirstOrDefault(x => x.Value == dummyorder.RequiredRouteOriginEntityKey).Key;

        //                            //Add session entity key to order for routings session with matching depots
        //                            foreach (DailyRoutingSession session in saveRoutingSession)
        //                            {
        //                                if (originDepotForOrder == session.Description)
        //                                {
        //                                    dummyorder.SessionEntityKey = session.EntityKey;
        //                                    //dummyorder.BeginDate = session.StartDate.ToString();
        //                                }
        //                            }

        //                            ordersToSaveInRNA.Add(dummyorder);



        //                            if (dummyorder.Tasks[0].ServiceWindowOverrides != null)
        //                            {
        //                                dummyorder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Add;
        //                            }

        //                            else
        //                            {
        //                                _Logger.DebugFormat("Order {0} with Service Location {1} not Found in RNA", dummyorder.Identifier, dummyorder.Tasks[0].LocationIdentifier);
        //                            }

        //                        }



        //                    }
        //                    foreach (Order orderSave in ordersToSaveInRNA)
        //                    {
        //                        if (orderSave.Action == ActionType.Update)
        //                        {
        //                            convertedOrderSpecs.Add(ConvertOrderToOrderSpec(orderSave));
        //                        }
        //                        else if (orderSave.Action == ActionType.Add)
        //                        {
        //                            var temp = ConvertOrderToOrderSpec(orderSave);
        //                            temp.OrderInstance = null;
        //                            convertedOrderSpecs.Add(temp);
        //                        }

        //                    }




        //                }
        //                catch (Exception ex)
        //                {
        //                    _Logger.Error("An error has occured during saving Orders" + ex.Message);
        //                }
        //            }

        //            else  //Orders have a been found in RNA
        //            {
        //                // Order Checked List and Returned RNA List
        //                dummyOrdersFromCSV.OrderBy(x => x.Identifier); // New Orders
        //                rnaOrders.OrderBy(x => x.Identifier); //Orders in RNA
        //                long[] regionEntityKey = new long[] { _Region.EntityKey };
        //                List<Order> updateOrders = new List<Order>(); //Orders to Update in RNA
        //                List<Order> regOrders = new List<Order>(); //Orders to Add to RNA
        //                List<Order> saveOrdersList = new List<Order>();
        //                ErrorLevel errorLevel = ErrorLevel.None;
        //                List<ServiceLocation> serviceLocationsforOrdersInRegion = RetrieveServiceLocationsByRegion(out errorLevel, out fatalErrorMessage, regionEntityKey).ToList();

        //                List<Order> dummyOrdersInRnaOrders = dummyOrdersFromCSV.Where(x => rnaOrders.Any(y => y.Identifier == x.Identifier)).ToList();
        //                List<Order> dummyOrdersNotInRnaOrders = dummyOrdersFromCSV.Where(x => !rnaOrders.Any(y => y.Identifier == x.Identifier)).ToList();

        //                foreach (Order dummyorder in dummyOrdersInRnaOrders)
        //                {
        //                    Order matchingRNAOrder = rnaOrders.Where(order => order.Identifier == dummyorder.Identifier).First();
        //                    dummyorder.Action = ActionType.Update;
        //                    dummyorder.EntityKey = matchingRNAOrder.EntityKey;
        //                    dummyorder.Version = matchingRNAOrder.Version;

        //                    if (matchingRNAOrder.Tasks != null && dummyorder.Tasks != null)
        //                    {
        //                        if (dummyorder.Tasks[0].ServiceWindowOverrides != null && matchingRNAOrder.Tasks[0].ServiceWindowOverrides != null)
        //                        {
        //                            dummyorder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
        //                            dummyorder.Tasks[0].ServiceWindowOverrides[0].EntityKey = matchingRNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;

        //                        }

        //                    }
        //                    ServiceLocation tempLocation = serviceLocationsforOrdersInRegion.FirstOrDefault(x => x.Identifier.ToUpper() == dummyorder.Tasks[0].LocationIdentifier.ToUpper());
        //                    if (tempLocation == null) // order service location found in RNA service locations
        //                    {
        //                        _Logger.DebugFormat("Could not find service location {0} for Dummy Order {1} in RNA", dummyorder.Tasks[0].LocationIdentifier, dummyorder.Identifier);
        //                    }
        //                    else // order service location found in RNA service locations
        //                    {
        //                        dummyorder.Coordinate = tempLocation.Coordinate;
        //                        dummyorder.Tasks[0].LocationEntityKey = tempLocation.EntityKey;
        //                        dummyorder.Tasks[0].LocationIdentifier = tempLocation.Identifier;
        //                        dummyorder.Tasks[0].LocationAddress = tempLocation.Address;
        //                        dummyorder.Tasks[0].LocationCoordinate = tempLocation.Coordinate;



        //                        string originDepotForOrder = RetrieveDepotsForRegionDict.FirstOrDefault(x => x.Value == dummyorder.RequiredRouteOriginEntityKey).Key;

        //                        //Add session entity key to order for routings session with matching depots
        //                        foreach (DailyRoutingSession session in saveRoutingSession)
        //                        {
        //                            if (originDepotForOrder == session.Description)
        //                            {
        //                                dummyorder.SessionEntityKey = session.EntityKey;
        //                                //dummyorder.BeginDate = session.StartDate.ToString();
        //                            }
        //                        }
        //                        updateOrders.Add(dummyorder);
        //                    }
        //                }
        //                foreach (Order dummyorder in dummyOrdersNotInRnaOrders)
        //                {
        //                    dummyorder.Action = ActionType.Add;
        //                    dummyorder.Tasks[0].TaskType_Type = "Pickup";
        //                    if (dummyorder.Tasks[0].ServiceWindowOverrides != null)
        //                    {
        //                        dummyorder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Add;
        //                    }


        //                    string originDepotForOrder = RetrieveDepotsForRegionDict.FirstOrDefault(x => x.Value == dummyorder.RequiredRouteOriginEntityKey).Key;

        //                    //Add session entity key to order for routings session with matching depots
        //                    foreach (DailyRoutingSession session in saveRoutingSession)
        //                    {
        //                        if (originDepotForOrder == session.Description)
        //                        {
        //                            dummyorder.SessionEntityKey = session.EntityKey;
        //                            //dummyorder.BeginDate = session.StartDate.ToString();
        //                        }
        //                    }

        //                    ServiceLocation tempLocation = serviceLocationsforOrdersInRegion.FirstOrDefault(x => x.Identifier.ToUpper() == dummyorder.Tasks[0].LocationIdentifier.ToUpper());
        //                    if (tempLocation == null) // order service location found in RNA service locations
        //                    {
        //                        _Logger.DebugFormat("Could not find service location {0} for Dummy Order {1} in RNA", dummyorder.Tasks[0].LocationIdentifier, dummyorder.Identifier);
        //                    }
        //                    else
        //                    {

        //                        dummyorder.Tasks[0].LocationEntityKey = tempLocation.EntityKey;
        //                        dummyorder.Tasks[0].LocationIdentifier = tempLocation.Identifier;
        //                        dummyorder.Tasks[0].LocationAddress = tempLocation.Address;
        //                        dummyorder.Tasks[0].LocationCoordinate = tempLocation.Coordinate;




        //                        regOrders.Add(dummyorder);
        //                    }
        //                }


        //                //Add Update and Reg orders to a list

        //                saveOrdersList = updateOrders.Concat(regOrders).ToList();

        //                //Convert Order to OrderSpec
        //                foreach (Order order in saveOrdersList)
        //                {
        //                    if (order.Action == ActionType.Update)
        //                    {
        //                        convertedOrderSpecs.Add(ConvertOrderToOrderSpec(order));
        //                    }
        //                    else if (order.Action == ActionType.Add)
        //                    {
        //                        var temp = ConvertOrderToOrderSpec(order);
        //                        temp.OrderInstance = null;
        //                        convertedOrderSpecs.Add(temp);
        //                    }

        //                }
        //            }

        //            //Save Dummy Orders to RNA if Matching orders Exist in RNA

        //            try
        //            {


        //                ErrorLevel errorLevel = ErrorLevel.None;
        //                foreach (OrderSpec order in convertedOrderSpecs)
        //                {
        //                    order.RegionEntityKey = _Region.EntityKey;
        //                    order.ManagedByUserEntityKey = MainService.User.EntityKey;


        //                };




        //                SaveResult[] savedOrdersResult = SaveOrders(out errorLevel, out fatalErrorMessage, convertedOrderSpecs.ToArray());



        //                if (errorLevel == ApexConsumer.ErrorLevel.Fatal)
        //                {
        //                    _Logger.Debug("Fatel Error Occured Saving Dummy Order  : " + fatalErrorMessage);
        //                }
        //                else
        //                {


        //                    foreach (SaveResult result in savedOrdersResult)
        //                    {
        //                        var tempOrder = (Order)result.Object;
        //                        if (result.Error == null)
        //                        {
        //                            _Logger.DebugFormat("Successfully Saved/Updated Dummy Order  {0} to RNA", tempOrder.Identifier);
        //                        }
        //                        else if (result.Error.ValidationFailures != null)
        //                        {
        //                            _Logger.ErrorFormat("An error has occured during saving/updating Dummy Order . The Dummy Order  {0} property {1} is not valid or in the proper format", tempOrder.Identifier, result.Error.ValidationFailures[0].Property);
        //                        }
        //                        else if (result.Error != null)
        //                        {
        //                            _Logger.ErrorFormat("An error has occured during saving/updating Dummy Order . The Dummy Order  {0} has the following error {1}: {2}", tempOrder.Identifier, result.Error.Code.ErrorCode_Status, result.Error.Detail);
        //                        }

        //                    }
        //                    _Logger.Debug("Saving Dummy Order Completed");
        //                }

        //            }
        //            catch (Exception ex)
        //            {
        //                _Logger.Error(ex.Message);
        //                errorRetrieveOrdersFromCSV = true;
        //                errorRetrieveOrdersFromCSVMessage = ex.Message;
        //            }

        //        }


        //    }
        //    catch (Exception ex)
        //    {
        //        _Logger.Error(ex.Message);
        //    }

        //}

        #endregion


    }
}
