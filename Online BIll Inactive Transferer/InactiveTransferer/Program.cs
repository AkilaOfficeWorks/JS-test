using System;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Data;
using System.Net;
using System.Collections.Specialized;
using System.Text;
using System.Net.Http;
using System.IO;
using System.Data.SqlTypes;
using System.Drawing;

//Original

namespace SMSSender
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            DatabaseCaller();
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Exit();
            System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "Inactive Transferer Ended - " + DateTime.Now.ToString() + "\n");
        }

        public class Mymodel
        {
            public string UserId { get; set; }
            public int[] subCategories { get; set; }
        }

        private static void DatabaseCaller()
        {
            string str1 = "Inactive Transferer Started - " + DateTime.Now.ToString();
            System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "-----------------------------------------------------------------------\n");
            System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", str1 + "\n");
            try
            {
                int j = 0;
                DataTable BillDataTable = new DataTable();
                String BillingString = "server=10.3.0.212;database=NWSDBCOM;UID=OnlineBill;password=OnlineBill~Bill#212@$";
                //String BillingString = "server=10.0.0.167\\WBSQLBILLHO2;database=NWSDBCOM;UID=sa;password=Com@bIll#IT";
                //Get the Inactive Accounts Dataset
                String query_get_data = "select cd.RegionId, cd.AreaId, cb.ConnectionId, cd.AccountNo, BillNo, BillYear, BillMonth, cd.CycleId, cd.WaterTarriffId, cd.MeterNo, cd.MeterStatusId, cd.MeterSizeId, " +
                    "DiameterSizeId, cd.ConnectionStatus, cd.AccountStatusId, cd.FacilityType,PrintingDate, CurrentReading, CurrentDate, CurrentStatus, ComplaintCode, PreviousReading, PreviousDate, PreviousStatus, " +
                    "CurrentConsumption, DailyAverage, DailyAverage1, DailyAverage2, DailyAverage3, cd.WalkingOrder,MeterReaderId, PaymentCutOffDate, LastPaymentDate, AmountBF, Adjustment1, Adjustment2, TotalPayment, " +
                    "PromptRebate, LastMonthDue, WaterCharge, ServiceCharge, Concession, Surcharge, Instalment, EnteredValue,CalculatedValue, AdvanceRebate, BillVAT, SewUsageCharge, SewServiceCharge, SewerageVAT, " +
                    "CurrentLateCharge, CumulativeLateCharge, ExtraConsumption, ExtraUsageCharges, CurrentMonthCharge, TotalDue, BalanceBF,InstallmentChargeNext, CurrentLateChargeNext, CumulativeLateChargeNext, " +
                    "SewUsageChargeNext, SewServiceChargeNext, SewerageVATNext, CFPayment, IsReserved, IdentityKey, IsCompleted, Detailed_Bill, IsRedBill, ElectricityCharge, ElectricityChargeNext " +
                    "from HistoryBillDetails cb, ConsumerCategories c, connectiondetails cd, consumerprofiles cp, MeterTypes m, BillingCycles bc " +
                    "where cd.WaterTarriffId=c.Categoryid and cb.Connectionid=cd.Connectionid and cd.Consumerid=cp.Consumerid and cd.MeterTypeId=m.MeterTypeId and bc.CycleId = cb.CycleId " +
                    "and BillYear = 2024 and BillMonth = 10 and cd.ConnectionStatus = 0 and cd.IsFinalized != 1 and cb.ConnectionId not in (SELECT ConnectionId FROM HistoryBillDetails WHERE (BillYear = 2024) AND (BillMonth = 11))";
                //String query_get_data = "select HistBillId, cd.RegionId, cd.AreaId, cb.ConnectionId, cd.AccountNo, BillNo, BillYear, BillMonth, cd.CycleId, cd.WaterTarriffId, cd.MeterNo, cd.MeterStatusId, cd.MeterSizeId, DiameterSizeId, cd.ConnectionStatus, cd.AccountStatusId, cd.FacilityType,PrintingDate, CurrentReading, CurrentDate, CurrentStatus, ComplaintCode, PreviousReading, PreviousDate, PreviousStatus, CurrentConsumption, DailyAverage, DailyAverage1, DailyAverage2, DailyAverage3, cd.WalkingOrder,MeterReaderId, PaymentCutOffDate, LastPaymentDate, AmountBF, Adjustment1, Adjustment2, TotalPayment, PromptRebate, LastMonthDue, WaterCharge, ServiceCharge, Concession, Surcharge, Instalment, EnteredValue,CalculatedValue, AdvanceRebate, BillVAT, SewUsageCharge, SewServiceCharge, SewerageVAT, CurrentLateCharge, CumulativeLateCharge, ExtraConsumption, ExtraUsageCharges, CurrentMonthCharge, TotalDue, BalanceBF,InstallmentChargeNext, CurrentLateChargeNext, CumulativeLateChargeNext, SewUsageChargeNext, SewServiceChargeNext, SewerageVATNext, CFPayment, IsReserved, IdentityKey, IsCompleted, IsRedBill from HistoryBillDetails cb, ConsumerCategories c, connectiondetails cd, consumerprofiles cp, MeterTypes m, BillingCycles bc where cd.WaterTarriffId=c.Categoryid and cb.Connectionid=cd.Connectionid and cd.Consumerid=cp.Consumerid and cd.MeterTypeId=m.MeterTypeId and bc.CycleId = cb.CycleId and BillYear = 2023 and BillMonth = 10 and cb.AreaId = 1254 and cb.ConnectionStatus = 0";

                SqlConnection conBilling = new SqlConnection(BillingString);
                SqlCommand cmdM = new SqlCommand(query_get_data, conBilling);
                cmdM.CommandTimeout = 360;
                conBilling.Open();
                SqlDataReader MyReader = cmdM.ExecuteReader();
                if (MyReader.Read())
                {
                    MyReader.Close();
                    MyReader.Dispose();
                    SqlDataAdapter myadapter = new SqlDataAdapter(cmdM);
                    myadapter.Fill(BillDataTable);
                    conBilling.Close();
                    int noofrows_enterd = 0;
                    int noofrows = BillDataTable.Rows.Count;
                    System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "Load " + noofrows.ToString() + " records - " + DateTime.Now.ToString() + "\n");
                    while (j < noofrows)
                    {
                        int ConnectionId = Convert.ToInt32(BillDataTable.Rows[j]["ConnectionId"].ToString());
                        try
                        {
                            string message = "";
                            string return_value = "";
                            DateTime? LastPaymentDate = null;
                            //int ConnectionId = Convert.ToInt32(BillDataTable.Rows[j]["ConnectionId"].ToString());
                            int RegionId = Convert.ToInt32(BillDataTable.Rows[j]["RegionId"].ToString());
                            int AreaId = Convert.ToInt32(BillDataTable.Rows[j]["AreaId"].ToString());
                            string AccountNo = BillDataTable.Rows[j]["AccountNo"].ToString();
                            int BillNo = Convert.ToInt32(BillDataTable.Rows[j]["BillNo"].ToString());
                            int PreviousYear = Convert.ToInt32(BillDataTable.Rows[j]["BillYear"].ToString());
                            int PreviousMonth = Convert.ToInt32(BillDataTable.Rows[j]["BillMonth"].ToString());
                            int CycleId = Convert.ToInt32(BillDataTable.Rows[j]["CycleId"].ToString());
                            int WaterTarriffId = Convert.ToInt32(BillDataTable.Rows[j]["WaterTarriffId"].ToString());
                            string MeterNo = BillDataTable.Rows[j]["MeterNo"].ToString();
                            int MeterStatusId = Convert.ToInt32(BillDataTable.Rows[j]["MeterStatusId"].ToString());
                            int MeterSizeId = Convert.ToInt32(BillDataTable.Rows[j]["MeterSizeId"].ToString());
                            int DiameterSizeId = Convert.ToInt32(BillDataTable.Rows[j]["DiameterSizeId"].ToString());
                            Boolean ConnectionStatus = Convert.ToBoolean(BillDataTable.Rows[j]["ConnectionStatus"].ToString());
                            int AccountStatusId = Convert.ToInt32(BillDataTable.Rows[j]["AccountStatusId"].ToString());
                            string FacilityType = BillDataTable.Rows[j]["FacilityType"].ToString();
                            int CurrentReading = Convert.ToInt32(BillDataTable.Rows[j]["CurrentReading"].ToString());
                            string CurrentStatus = BillDataTable.Rows[j]["CurrentStatus"].ToString();
                            DateTime CurrentDateReading = Convert.ToDateTime(BillDataTable.Rows[j]["CurrentDate"].ToString());
                            Decimal DailyAverage = Convert.ToDecimal(BillDataTable.Rows[j]["DailyAverage"].ToString());
                            Decimal DailyAverage1 = Convert.ToDecimal(BillDataTable.Rows[j]["DailyAverage1"].ToString());
                            Decimal DailyAverage2 = Convert.ToDecimal(BillDataTable.Rows[j]["DailyAverage2"].ToString());
                            Decimal WalkingOrder = Convert.ToDecimal(BillDataTable.Rows[j]["WalkingOrder"].ToString());
                            int MeterReaderId = Convert.ToInt32(BillDataTable.Rows[j]["MeterReaderId"].ToString());
                            decimal Dues_Previous_Month = Convert.ToDecimal(BillDataTable.Rows[j]["TotalDue"].ToString());
                            decimal InstallmentChargeNext = Convert.ToDecimal(BillDataTable.Rows[j]["InstallmentChargeNext"].ToString());
                            string LastPaymentDateString = BillDataTable.Rows[j]["LastPaymentDate"].ToString();
                            if (LastPaymentDateString != "")
                            {
                                LastPaymentDate = Convert.ToDateTime(BillDataTable.Rows[j]["LastPaymentDate"].ToString());
                            }


                            decimal MonthlyAmount, BalanceAmount, LastAmount, DRJournal, CRJournal, TotAdjustment2 = 0, TotAdjustment1 = 0, Surcharge = 0, TotalPayment = 0, InstallmentCharge = 0.00M, PreviousPromptRebate = 0;
                            decimal CurrentMonthCharge = 0, LastMonthDue = 0, PromptRebate = 0, AdvanceRebate = 0;
                            DateTime? PaymentCutOffDate = null, PaymentDate = null, CurrentDate = new DateTime(2024, 11, 30);
                            decimal Total_Amount_Due = 0, Adjustment1 = 0, Adjustment2 = 0, TotalDue = 0, BalanceBF = 0, Calculated_enterd_Value = 0, Total_Amount_Due_with_Payment = 0;
                            SqlDateTime sqldatenull;
                            int Year, Month;
                            Decimal WaterCharge_local_for_Actual_Days = 0, ServiceCharges_local_for_Actual_Days = 0, Concession = 0, Water_VAT = 0, SeweUsageCharge = 0, SeweServiceCharge = 0, SewVATAmount = 0;
                            Decimal CurrentLateChargeNext = 0, CumulativeLateChargeNext = 0;

                            //Generate time stamp key
                            long ticks = DateTime.Now.Ticks;
                            byte[] bytes = BitConverter.GetBytes(ticks);
                            string id = Convert.ToBase64String(bytes).Replace('+', '_').Replace('/', '-').TrimEnd('=');
                            id = id.Substring(0, 10);
                            //end

                            //Reff Year/Month
                            if (PreviousMonth == 12)
                            {
                                Month = 1;
                                Year = PreviousYear + 1;
                            }
                            else
                            {
                                Month = PreviousMonth + 1;
                                Year = PreviousYear;
                            }



                            Total_Amount_Due = Dues_Previous_Month;
                            //Payment 
                            //Calculate Journal (Bill Adjustments) / Surcharge
                            UnAppliedJournalDetails return_values_UnAppliedJournalDetails = Get_UnAppliedJournalDetails(ConnectionId);
                            DRJournal = return_values_UnAppliedJournalDetails.DRJournal;
                            CRJournal = return_values_UnAppliedJournalDetails.CRJournal;
                            Surcharge = return_values_UnAppliedJournalDetails.Surcharge;

                            Adjustment2 = DRJournal - CRJournal;
                            //End  - Calculate Journal (Bill Adjustments) / Surcharge

                            //Update UnAppliedJournalDetails table
                            update_UnAppliedJournalDetails(ConnectionId);
                            //End

                            //Calculate Payments
                            TotalPayment = 0.00M;
                            UnAppliedPaymentDetails return_values_UnAppliedPaymentDetails = Get_UnAppliedPaymentDetails(ConnectionId, LastPaymentDate);
                            TotalPayment = return_values_UnAppliedPaymentDetails.TotalPayment;
                            PaymentDate = return_values_UnAppliedPaymentDetails.PaymentDate;
                            //End

                            //Update UnAppliedPaymentDetails table
                            update_UnAppliedPaymentDetails(ConnectionId);
                            //End

                            //Calculation
                            CurrentMonthCharge = Surcharge;
                            LastMonthDue = Dues_Previous_Month + Adjustment2 - TotalPayment;
                            TotalDue = CurrentMonthCharge + LastMonthDue;
                            BalanceBF = CurrentMonthCharge + Dues_Previous_Month;
                            Calculated_enterd_Value = BalanceBF - Surcharge;
                            Total_Amount_Due_with_Payment = Total_Amount_Due + Adjustment2 + Surcharge - TotalPayment;


                            //Save final details to History Bill Details in Billing System
                            SqlConnection myconn_billing_live = new SqlConnection(BillingString);
                            SqlCommand MyCommand_billing_liv = new SqlCommand("", myconn_billing_live);
                            MyCommand_billing_liv.CommandTimeout = 360;
                            try
                            {
                                myconn_billing_live.Open();

                                sqldatenull = SqlDateTime.Null;
                                MyCommand_billing_liv.Parameters.Add("@RegionId", SqlDbType.Int).Value = RegionId;
                                MyCommand_billing_liv.Parameters.Add("@AreaId", SqlDbType.Int).Value = AreaId;
                                MyCommand_billing_liv.Parameters.Add("@ConnectionId", SqlDbType.Int).Value = ConnectionId;
                                MyCommand_billing_liv.Parameters.Add("@AccountNo", SqlDbType.VarChar).Value = AccountNo;
                                MyCommand_billing_liv.Parameters.Add("@BillNo", SqlDbType.Int).Value = BillNo + 1;
                                MyCommand_billing_liv.Parameters.Add("@BillYear", SqlDbType.Int).Value = Year;
                                MyCommand_billing_liv.Parameters.Add("@BillMonth", SqlDbType.Int).Value = Month;
                                MyCommand_billing_liv.Parameters.Add("@CycleId", SqlDbType.Int).Value = CycleId;
                                MyCommand_billing_liv.Parameters.Add("@WaterTarriffId", SqlDbType.Int).Value = WaterTarriffId;

                                MyCommand_billing_liv.Parameters.Add("@MeterNo", SqlDbType.VarChar).Value = MeterNo;
                                MyCommand_billing_liv.Parameters.Add("@MeterStatusId", SqlDbType.Int).Value = MeterStatusId;
                                MyCommand_billing_liv.Parameters.Add("@MeterSizeId", SqlDbType.Int).Value = MeterSizeId;
                                MyCommand_billing_liv.Parameters.Add("@ConnectionSizeId", SqlDbType.Int).Value = DiameterSizeId;
                                MyCommand_billing_liv.Parameters.Add("@ConnectionStatus", SqlDbType.Bit).Value = ConnectionStatus;
                                MyCommand_billing_liv.Parameters.Add("@AccountStatusId", SqlDbType.Int).Value = AccountStatusId;
                                MyCommand_billing_liv.Parameters.Add("@FacilityType", SqlDbType.VarChar).Value = FacilityType;
                                MyCommand_billing_liv.Parameters.Add("@PrintingDate", SqlDbType.DateTime).Value = CurrentDate;
                                MyCommand_billing_liv.Parameters.Add("@CurrentReading", SqlDbType.Int).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@CurrentDate", SqlDbType.DateTime).Value = CurrentDate;

                                MyCommand_billing_liv.Parameters.Add("@CurrentStatus", SqlDbType.VarChar).Value = "E";
                                MyCommand_billing_liv.Parameters.Add("@ComplaintCode", SqlDbType.Int).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@PreviousReading", SqlDbType.Int).Value = CurrentReading;
                                MyCommand_billing_liv.Parameters.Add("@PreviousDate", SqlDbType.DateTime).Value = CurrentDateReading;
                                MyCommand_billing_liv.Parameters.Add("@PreviousStatus", SqlDbType.VarChar).Value = CurrentStatus;
                                MyCommand_billing_liv.Parameters.Add("@CurrentConsumption", SqlDbType.Int).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@DailyAverage", SqlDbType.Decimal).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@DailyAverage1", SqlDbType.Decimal).Value = DailyAverage;
                                MyCommand_billing_liv.Parameters.Add("@DailyAverage2", SqlDbType.Decimal).Value = DailyAverage1;
                                MyCommand_billing_liv.Parameters.Add("@DailyAverage3", SqlDbType.Decimal).Value = DailyAverage2;

                                MyCommand_billing_liv.Parameters.Add("@WalkingOrder", SqlDbType.Decimal).Value = WalkingOrder;
                                MyCommand_billing_liv.Parameters.Add("@MeterReaderId", SqlDbType.Int).Value = MeterReaderId;
                                MyCommand_billing_liv.Parameters.Add("@PaymentCutOffDate", SqlDbType.DateTime).Value = DateTime.Now.AddDays(14);
                                if (LastPaymentDate == null)
                                {
                                    MyCommand_billing_liv.Parameters.Add("@LastPaymentDate", SqlDbType.DateTime).Value = sqldatenull;
                                }
                                else
                                {
                                    MyCommand_billing_liv.Parameters.Add("@LastPaymentDate", SqlDbType.DateTime).Value = PaymentDate;
                                }
                                MyCommand_billing_liv.Parameters.Add("@AmountBF", SqlDbType.Decimal).Value = Dues_Previous_Month; // Orevious month Total Due
                                MyCommand_billing_liv.Parameters.Add("@Adjustment1", SqlDbType.Decimal).Value = Adjustment1;
                                MyCommand_billing_liv.Parameters.Add("@Adjustment2", SqlDbType.Decimal).Value = Adjustment2;
                                MyCommand_billing_liv.Parameters.Add("@TotalPayment", SqlDbType.Decimal).Value = TotalPayment;
                                MyCommand_billing_liv.Parameters.Add("@PromptRebate", SqlDbType.Decimal).Value = PromptRebate;
                                MyCommand_billing_liv.Parameters.Add("@LastMonthDue", SqlDbType.Decimal).Value = LastMonthDue;

                                MyCommand_billing_liv.Parameters.Add("@WaterCharge", SqlDbType.Decimal).Value = Math.Round(WaterCharge_local_for_Actual_Days, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@ServiceCharge", SqlDbType.Decimal).Value = Math.Round(ServiceCharges_local_for_Actual_Days, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@Concession", SqlDbType.Decimal).Value = Concession;
                                MyCommand_billing_liv.Parameters.Add("@Surcharge", SqlDbType.Decimal).Value = Math.Round(Surcharge, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@Instalment", SqlDbType.Decimal).Value = InstallmentChargeNext;
                                MyCommand_billing_liv.Parameters.Add("@EnteredValue", SqlDbType.Decimal).Value = Math.Round(Calculated_enterd_Value, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@CalculatedValue", SqlDbType.Decimal).Value = Math.Round(Calculated_enterd_Value, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@AdvanceRebate", SqlDbType.Decimal).Value = AdvanceRebate;
                                MyCommand_billing_liv.Parameters.Add("@BillVAT", SqlDbType.Decimal).Value = Water_VAT;
                                MyCommand_billing_liv.Parameters.Add("@SewUsageCharge", SqlDbType.Decimal).Value = Math.Round(SeweUsageCharge, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@SewServiceCharge", SqlDbType.Decimal).Value = Math.Round(SeweServiceCharge, 2, MidpointRounding.AwayFromZero);

                                MyCommand_billing_liv.Parameters.Add("@SewerageVAT", SqlDbType.Decimal).Value = Math.Round(SewVATAmount, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@CurrentLateCharge", SqlDbType.Decimal).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@CumulativeLateCharge", SqlDbType.Decimal).Value = 0; //0 From all time
                                MyCommand_billing_liv.Parameters.Add("@ExtraConsumption", SqlDbType.Decimal).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@ExtraUsageCharges", SqlDbType.Decimal).Value = 0;
                                MyCommand_billing_liv.Parameters.Add("@CurrentMonthCharge", SqlDbType.Decimal).Value = Math.Round(CurrentMonthCharge, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@TotalDue", SqlDbType.Decimal).Value = Math.Round(TotalDue, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@BalanceBF", SqlDbType.Decimal).Value = Math.Round(BalanceBF, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@InstallmentChargeNext", SqlDbType.Decimal).Value = Math.Round(InstallmentChargeNext, 2, MidpointRounding.AwayFromZero);

                                MyCommand_billing_liv.Parameters.Add("@CurrentLateChargeNext", SqlDbType.Decimal).Value = Math.Round(CurrentLateChargeNext, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@CumulativeLateChargeNext", SqlDbType.Decimal).Value = Math.Round(CumulativeLateChargeNext, 2, MidpointRounding.AwayFromZero);
                                MyCommand_billing_liv.Parameters.Add("@SewUsageChargeNext", SqlDbType.Decimal).Value = Math.Round(SeweUsageCharge, 2, MidpointRounding.AwayFromZero); //No need for Online Billing
                                MyCommand_billing_liv.Parameters.Add("@SewServiceChargeNext", SqlDbType.Decimal).Value = Math.Round(SeweServiceCharge, 2, MidpointRounding.AwayFromZero); //No need for Online Billing
                                MyCommand_billing_liv.Parameters.Add("@SewerageVATNext", SqlDbType.Decimal).Value = Math.Round(SewVATAmount, 2, MidpointRounding.AwayFromZero); //No need for Online Billing
                                MyCommand_billing_liv.Parameters.Add("@CFPayment", SqlDbType.Decimal).Value = 0; //0 From all time
                                MyCommand_billing_liv.Parameters.Add("@IdentityKey", SqlDbType.VarChar).Value = id; //Time Stamp Key
                                MyCommand_billing_liv.Parameters.Add("@Detailed_Bill", SqlDbType.VarChar).Value = ""; //Time Stamp Key
                                MyCommand_billing_liv.Parameters.Add("@IsRedBill", SqlDbType.Int).Value = 0; //Red Bill Status

                                MyCommand_billing_liv.CommandText = "select HistBillId from HistoryBillDetails where BillYear = @BillYear and BillMonth = @BillMonth and ConnectionId = @ConnectionId";
                                SqlDataReader Billing_Histry_Bill = MyCommand_billing_liv.ExecuteReader();
                                if (Billing_Histry_Bill.Read())
                                {
                                    Billing_Histry_Bill.Close();
                                    MyCommand_billing_liv.Parameters.Clear();
                                    System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "Already Exist Record - " + ConnectionId.ToString() + " - " + DateTime.Now.ToString() + "\n");
                                }
                                else
                                {
                                    Billing_Histry_Bill.Close();
                                    MyCommand_billing_liv.CommandText = "if not EXISTS (select * from HistoryBillDetails where BillYear = @BillYear and BillMonth = @BillMonth and ConnectionId = @ConnectionId)" +
                                    "insert into HistoryBillDetails(RegionId,AreaId,ConnectionId,AccountNo,BillNo,BillYear,BillMonth,CycleId,WaterTarriffId," +
                                    "MeterNo,MeterStatusId,MeterSizeId,DiameterSizeId,ConnectionStatus,AccountStatusId,FacilityType,PrintingDate,CurrentReading,CurrentDate," +
                                    "CurrentStatus,ComplaintCode,PreviousReading,PreviousDate,PreviousStatus,CurrentConsumption,DailyAverage,DailyAverage1,DailyAverage2,DailyAverage3," +
                                    "WalkingOrder,MeterReaderId,PaymentCutOffDate,LastPaymentDate,AmountBF,Adjustment1,Adjustment2,TotalPayment,PromptRebate,LastMonthDue," +
                                    "WaterCharge,ServiceCharge,Concession,Surcharge,Instalment,EnteredValue,CalculatedValue,AdvanceRebate,BillVAT,SewUsageCharge," +
                                    "SewServiceCharge,SewerageVAT,CurrentLateCharge,CumulativeLateCharge,ExtraConsumption,ExtraUsageCharges,CurrentMonthCharge,TotalDue," +
                                    "BalanceBF,InstallmentChargeNext,CurrentLateChargeNext,CumulativeLateChargeNext,SewUsageChargeNext,SewServiceChargeNext,SewerageVATNext,CFPayment,IdentityKey,IsRedBill) " +
                                    "values (@RegionId,@AreaId,@ConnectionId,@AccountNo,@BillNo,@BillYear,@BillMonth,@CycleId,@WaterTarriffId," +
                                    "@MeterNo,@MeterStatusId,@MeterSizeId,@ConnectionSizeId,@ConnectionStatus,@AccountStatusId,@FacilityType,@PrintingDate,@CurrentReading,@CurrentDate," +
                                    "@CurrentStatus,@ComplaintCode,@PreviousReading,@PreviousDate,@PreviousStatus,@CurrentConsumption,@DailyAverage,@DailyAverage1,@DailyAverage2,@DailyAverage3," +
                                    "@WalkingOrder,@MeterReaderId,@PaymentCutOffDate,@LastPaymentDate,@AmountBF,@Adjustment1,@Adjustment2,@TotalPayment,@PromptRebate,@LastMonthDue," +
                                    "@WaterCharge,@ServiceCharge,@Concession,@Surcharge,@Instalment,@EnteredValue,@CalculatedValue,@AdvanceRebate,@BillVAT,@SewUsageCharge," +
                                    "@SewServiceCharge,@SewerageVAT,@CurrentLateCharge,@CumulativeLateCharge,@ExtraConsumption,@ExtraUsageCharges,@CurrentMonthCharge,@TotalDue," +
                                    "@BalanceBF,@InstallmentChargeNext,@CurrentLateChargeNext,@CumulativeLateChargeNext,@SewUsageChargeNext,@SewServiceChargeNext,@SewerageVATNext,@CFPayment,@IdentityKey,@IsRedBill)";
                                    MyCommand_billing_liv.ExecuteNonQuery();

                                    MyCommand_billing_liv.CommandText = "Update HistoryBillDetails set IsCompleted = 1 where ConnectionId = @ConnectionId and BillYear = " + PreviousYear + " and BillMonth = " + PreviousMonth + "";
                                    MyCommand_billing_liv.ExecuteNonQuery();

                                    MyCommand_billing_liv.CommandText = "update HistoryBillDetails set ComplaintCode = NULL where ComplaintCode = 0 and ConnectionId = @ConnectionId and BillYear = @BillYear and BillMonth = @BillMonth";
                                    MyCommand_billing_liv.ExecuteNonQuery();

                                    //Update details in HistoryJournal
                                    MyCommand_billing_liv.CommandText = "update h set AppliedAmount = h.Amount, AppliedBillYear = hb.BillYear, AppliedBillMonth = hb.BillMonth from UnAppliedJournalDetails u, HistoryBillDetails hb, HistoryJournal h where u.connectionid=hb.connectionid and u.connectionid=h.connectionId and u.AppliedForBill = 'True' and hb.ConnectionId = @ConnectionId and u.Amount = h.Amount and h.AppliedAmount is null and h.AppliedBillYear is null and h.AppliedBillMonth is null and hb.BillYear = @BillYear and hb.BillMonth = @BillMonth";
                                    MyCommand_billing_liv.ExecuteNonQuery();

                                    //Update details in HistoryPaymentDetails
                                    MyCommand_billing_liv.CommandText = "update h set AppliedBillYear = hb.BillYear, AppliedBillMonth = hb.BillMonth from UnAppliedPaymentDetails u, HistoryBillDetails hb, HistoryPaymentDetails h where u.ConnectionId = hb.ConnectionId and h.AccountNo = u.AccountNo and h.ConnectionId = u.ConnectionId and h.PaymentAmount = u.PaymentAmount and h.PaymentDate = u.PaymentDate and h.CollectionCentreId = u.CollectionCentreId and AppliedForBill = 'True' and hb.ConnectionId = @ConnectionId and hb.BillYear = @BillYear and hb.BillMonth = @BillMonth";
                                    MyCommand_billing_liv.ExecuteNonQuery();

                                    //Delete details from UnAppliedJournalDetails
                                    MyCommand_billing_liv.CommandText = "Delete u from UnAppliedJournalDetails u, ConnectionDetails c where u.connectionid = c.connectionid and c.ConnectionId = @ConnectionId and AppliedForBill = 'True'";
                                    MyCommand_billing_liv.ExecuteNonQuery();

                                    //Delete details from UnAppliedPaymentDetails
                                    MyCommand_billing_liv.CommandText = "Delete u from UnAppliedPaymentDetails u, ConnectionDetails c where u.connectionid = c.connectionid and c.ConnectionId = @ConnectionId and AppliedForBill = 'True'";
                                    MyCommand_billing_liv.ExecuteNonQuery();



                                    MyCommand_billing_liv.Dispose();
                                    MyCommand_billing_liv.Parameters.Clear();
                                    System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "Success - " + ConnectionId.ToString() + " - " + DateTime.Now.ToString() + "\n");
                                }
                            }
                            catch (Exception ex)
                            {
                                System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", ex.ToString() + ConnectionId.ToString() + DateTime.Now.ToString() + "\n");
                                continue;
                            }
                            finally
                            {
                                myconn_billing_live.Close();
                            }

                            j++;
                        }
                        catch (Exception ex)
                        {
                            System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", ex.ToString() + " - " + ConnectionId.ToString() + " - " + DateTime.Now.ToString() + "\n");
                            continue;
                        }
                    }

                }
                else
                {
                    conBilling.Close();
                    System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "No record found - " + DateTime.Now.ToString() + "\n");
                }
            }
            catch (Exception es)
            {
                MessageBox.Show(es.Message);
                System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", es.Message.ToString() + DateTime.Now.ToString() + "\n");
            }
        }

        class UnAppliedJournalDetails
        {
            public decimal DRJournal { get; set; }
            public decimal CRJournal { get; set; }
            public decimal Surcharge { get; set; }
        }

        private static void update_UnAppliedJournalDetails(int ConnectionId)
        {
            String BillingString = "server=10.3.0.212;database=NWSDBCOM;UID=OnlineBill;password=OnlineBill~Bill#212@$";
            //String BillingString = "server=10.0.0.167\\WBSQLBILLHO2;database=NWSDBCOM;UID=sa;password=Com@bIll#IT";
            SqlConnection myconn = new SqlConnection(BillingString);
            SqlCommand MyCommand = new SqlCommand("", myconn);
            MyCommand.CommandTimeout = 360;
            try
            {
                myconn.Open();
                MyCommand.Parameters.Add("@ConnectionId", SqlDbType.Int).Value = ConnectionId;
                MyCommand.CommandText = "update UnAppliedJournalDetails set AppliedForBill = 'True', AppliedDate = getdate() where ConnectionId = @ConnectionId";
                MyCommand.ExecuteNonQuery();
                MyCommand.Parameters.Clear();
                MyCommand.Dispose();
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "update_UnAppliedJournalDetails - " + ConnectionId.ToString() + DateTime.Now.ToString() + "\n");
            }
            finally
            {
                myconn.Close();
            }
        }

        private static UnAppliedJournalDetails Get_UnAppliedJournalDetails(int ConnectionId)
        {
            decimal DRJournal = 0, CRJournal = 0, Surcharge = 0;
            String BillingString = "server=10.3.0.212;database=NWSDBCOM;UID=OnlineBill;password=OnlineBill~Bill#212@$";
            //String BillingString = "server=10.0.0.167\\WBSQLBILLHO2;database=NWSDBCOM;UID=sa;password=Com@bIll#IT";
            SqlConnection myconn = new SqlConnection(BillingString);
            SqlCommand MyCommand = new SqlCommand("", myconn);
            MyCommand.CommandTimeout = 360;
            try
            {
                myconn.Open();
                MyCommand.Parameters.Add("@ConnectionId", SqlDbType.Int).Value = ConnectionId;
                MyCommand.CommandText = "select ISNULL((select sum(Amount) from UnAppliedJournalDetails where JournalType = 'A' and JournalAction = 'DR' and JournalCode not in (28,84) and ConnectionId = @ConnectionId),0) as DRJournal,ISNULL((select sum(Amount) from UnAppliedJournalDetails where JournalType = 'A' and JournalAction = 'CR' and ConnectionId = @ConnectionId),0) as CRJournal,ISNULL((select sum(Amount) from UnAppliedJournalDetails where JournalType = 'S' and ConnectionId = @ConnectionId),0) as Surcharge";
                SqlDataReader MyReader = MyCommand.ExecuteReader();
                MyCommand.Parameters.Clear();
                MyCommand.Dispose();
                if (MyReader.Read())
                {
                    DRJournal = Convert.ToDecimal(MyReader["DRJournal"]);
                    CRJournal = Convert.ToDecimal(MyReader["CRJournal"]);
                    Surcharge = Convert.ToDecimal(MyReader["Surcharge"]);
                    MyReader.Close();
                }
                else
                {
                    DRJournal = 0.00M;
                    CRJournal = 0.00M;
                    Surcharge = 0.00M;
                }
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "Get_UnAppliedJournalDetails - " + ConnectionId.ToString() + DateTime.Now.ToString() + "\n");
            }
            finally
            {
                myconn.Close();
            }

            return new UnAppliedJournalDetails()
            {
                DRJournal = DRJournal,
                CRJournal = CRJournal,
                Surcharge = Surcharge
            };
        }

        class UnAppliedPaymentDetails
        {
            public decimal TotalPayment { get; set; }
            public DateTime? PaymentDate { get; set; }
        }

        private static UnAppliedPaymentDetails Get_UnAppliedPaymentDetails(int ConnectionId, DateTime? LastPaymentDate)
        {
            decimal TotalPayment = 0;
            DateTime? PaymentDate = null;
            String BillingString = "server=10.3.0.212;database=NWSDBCOM;UID=OnlineBill;password=OnlineBill~Bill#212@$";
            //String BillingString = "server=10.0.0.167\\WBSQLBILLHO2;database=NWSDBCOM;UID=sa;password=Com@bIll#IT";
            SqlConnection myconn = new SqlConnection(BillingString);
            SqlCommand MyCommand = new SqlCommand("", myconn);
            MyCommand.CommandTimeout = 360;
            try
            {
                myconn.Open();
                MyCommand.Parameters.Add("@ConnectionId", SqlDbType.Int).Value = ConnectionId;
                MyCommand.CommandText = "select ISNULL(sum(PaymentAmount),0) as TotalPayment from UnAppliedPaymentDetails where ConnectionId = @ConnectionId";
                SqlDataReader MyReader = MyCommand.ExecuteReader();
                MyCommand.Parameters.Clear();
                MyCommand.Dispose();
                if (MyReader.Read())
                {
                    TotalPayment = Convert.ToDecimal(MyReader["TotalPayment"]);
                    MyReader.Close();
                }
                else
                {
                    TotalPayment = 0.00M;
                }
                MyCommand.Parameters.Add("@ConnectionId", SqlDbType.Int).Value = ConnectionId;
                MyCommand.CommandText = "select top(1) PaymentDate from UnAppliedPaymentDetails where ConnectionId = @ConnectionId order by PaymentDate desc";
                SqlDataReader MyReader1 = MyCommand.ExecuteReader();
                MyCommand.Dispose();
                if (MyReader1.Read())
                {
                    PaymentDate = Convert.ToDateTime(MyReader1["PaymentDate"]);
                    MyReader.Close();
                }
                else
                {
                    PaymentDate = LastPaymentDate;
                }
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "Get_UnAppliedPaymentDetails - " + ConnectionId.ToString() + DateTime.Now.ToString() + "\n");
            }
            finally
            {
                myconn.Close();
            }

            return new UnAppliedPaymentDetails()
            {
                TotalPayment = TotalPayment,
                PaymentDate = PaymentDate
            };
        }

        private static void update_UnAppliedPaymentDetails(int ConnectionId)
        {
            String BillingString = "server=10.3.0.212;database=NWSDBCOM;UID=OnlineBill;password=OnlineBill~Bill#212@$";
            //String BillingString = "server=10.0.0.167\\WBSQLBILLHO2;database=NWSDBCOM;UID=sa;password=Com@bIll#IT";
            SqlConnection myconn = new SqlConnection(BillingString);
            SqlCommand MyCommand = new SqlCommand("", myconn);
            MyCommand.CommandTimeout = 360;
            try
            {
                myconn.Open();
                MyCommand.Parameters.Add("@ConnectionId", SqlDbType.Int).Value = ConnectionId;
                MyCommand.CommandText = "update UnAppliedPaymentDetails set AppliedForBill = 'True', AppliedDate = getdate() where ConnectionId = @ConnectionId";
                MyCommand.ExecuteNonQuery();
                MyCommand.Parameters.Clear();
                MyCommand.Dispose();
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("C:\\Temp\\InactiveTransferer.txt", "update_UnAppliedPaymentDetails - " + ConnectionId.ToString() + DateTime.Now.ToString() + "\n");
            }
            finally
            {
                myconn.Close();
            }
        }
    }
}
