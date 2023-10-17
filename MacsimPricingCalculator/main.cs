#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net;
using System.Data.SqlClient;
using System.Data;
using System.Data.Odbc;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using System.Web.Http;

namespace MacsimPricingCalculator
{
    public static class main
    {
        [FunctionName("MacsimPricing")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string customerID = req.Query["customerID"];
            string? productID = req.Query["productID"];

            if (string.IsNullOrEmpty(customerID))
            {
                Console.WriteLine("Missing customerID input!");
                //return new HttpResponseMessage(HttpStatusCode.BadRequest);
                return new BadRequestErrorMessageResult("Missing customerID input!");
            }

            Console.WriteLine("######## Fetching Data");
            Console.WriteLine("Fetching Pricing");
            var pricingsGeneral = DBFunctions.FetchPricingGroup(null);

            if (pricingsGeneral.Count == 0)
            {
                Console.WriteLine("No General Pricings found");
                //return new HttpResponseMessage(HttpStatusCode.BadRequest);
                return new BadRequestErrorMessageResult("No General Pricings found!");
            }

            Console.WriteLine("Fetching Customers");
            var customers = DBFunctions.FetchCustomers(customerID);

            if (customers.Count == 0)
            {
                Console.WriteLine("No Customers found");
                //return new HttpResponseMessage(HttpStatusCode.BadRequest);
                return new BadRequestErrorMessageResult("No Customers found!");
            }

            string csv = CustomerProductPricing.GenerateCSVHeader();
            Console.WriteLine("######## Begin Main Loop");
            foreach (var customer in customers)
            {
                var products = DBFunctions.FetchProducts(productID);

                if (products.Count == 0)
                {
                    Console.WriteLine($"No Products found for customer {customer.ID}");
                    continue;
                }

                var pricingCustomerType = DBFunctions.FetchPricingGroup(customer.BCode);
                var pricingCustomerSpecific = DBFunctions.FetchPricingGroup(customer.ID);

                Console.WriteLine("Generating Pricings");

                foreach (var product in products)
                {
                    var pricing1 = pricingsGeneral.Find(x => x.Stock == product.Group && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing2 = pricingsGeneral.Find(x => x.Stock == product.Group && x.Type == Globals.TYPE_OVERRIDE);

                    var pricing3 = pricingsGeneral.Find(x => x.Stock == product.PCode && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing4 = pricingsGeneral.Find(x => x.Stock == product.PCode && x.Type == Globals.TYPE_OVERRIDE);


                    var pricing5 = pricingCustomerType.Find(x => x.Stock == product.Group && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing6 = pricingCustomerType.Find(x => x.Stock == product.Group && x.Type == Globals.TYPE_OVERRIDE);

                    var pricing7 = pricingCustomerType.Find(x => x.Stock == product.PCode && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing8 = pricingCustomerType.Find(x => x.Stock == product.PCode && x.Type == Globals.TYPE_OVERRIDE);


                    var pricing9 = pricingCustomerSpecific.Find(x => x.Stock == product.Group && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing10 = pricingCustomerSpecific.Find(x => x.Stock == product.Group && x.Type == Globals.TYPE_OVERRIDE);

                    var pricing11 = pricingCustomerSpecific.Find(x => x.Stock == product.PCode && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing12 = pricingCustomerSpecific.Find(x => x.Stock == product.PCode && x.Type == Globals.TYPE_OVERRIDE);


                    CustomerProductPricing customerProductPricing = new CustomerProductPricing(customer.ID, customer.BCode, product.PCode, product.Group);

                    // Set base price
                    switch (customer.PBook)
                    {
                        case "1":
                            customerProductPricing.PriceBase = (decimal)product.Sell1;
                            break;
                        case "2":
                            customerProductPricing.PriceBase = (decimal)product.Sell2;
                            break;
                        case "3":
                            customerProductPricing.PriceBase = (decimal)product.Sell3;
                            break;
                        case "4":
                            customerProductPricing.PriceBase = (decimal)product.Sell4;
                            break;
                        case "5":
                            customerProductPricing.PriceBase = (decimal)product.Sell5;
                            break;
                        case "6":
                            customerProductPricing.PriceBase = (decimal)product.Sell6;
                            break;
                        case "7":
                            customerProductPricing.PriceBase = (decimal)product.Sell7;
                            break;
                        case "8":
                            customerProductPricing.PriceBase = (decimal)product.Sell8;
                            break;
                        default:
                            throw new Exception("Invalid PBook value");
                    }

                    // Set all other prices

                    // Set all Price Overrides (type = 1)
                    {
                        customerProductPricing.Price2 = (decimal?)pricing2?.Rate;
                        customerProductPricing.Price4 = (decimal?)pricing4?.Rate;
                        customerProductPricing.Price6 = (decimal?)pricing6?.Rate;
                        customerProductPricing.Price8 = (decimal?)pricing8?.Rate;
                        customerProductPricing.Price10 = (decimal?)pricing10?.Rate;
                        customerProductPricing.Price12 = (decimal?)pricing12?.Rate;

                        // When we want to add checks for qty changes
                        //customerProductPricing.Price2 = pricing2.Count > 0 ? (decimal?)pricing2[0]?.Rate : null;
                        //customerProductPricing.Price4 = pricing4.Count > 0 ? (decimal?)pricing4[0]?.Rate : null;
                        //customerProductPricing.Price6 = pricing6.Count > 0 ? (decimal?)pricing6[0]?.Rate : null;
                        //customerProductPricing.Price8 = pricing8.Count > 0 ? (decimal?)pricing8[0]?.Rate : null;
                        //customerProductPricing.Price10 = pricing10.Count > 0 ? (decimal?)pricing10[0]?.Rate : null;
                        //customerProductPricing.Price12 = pricing12.Count > 0 ? (decimal?)pricing12[0]?.Rate : null;
                    }

                    // Set all discounts (type = 2)
                    {
                        customerProductPricing.Price1 = pricing1?.Rate == null ? null : customerProductPricing.PriceBase * (decimal?)(1 - pricing1.Rate / 100);
                        if (customerProductPricing.Price1 != null) { customerProductPricing.Price1 = Math.Round((decimal)customerProductPricing.Price1, Globals.SOLUTION_NUM_DECIMAL_PLACES); }

                        customerProductPricing.Price3 = pricing3?.Rate == null ? null : customerProductPricing.PriceBase * (decimal?)(1 - pricing3.Rate / 100);
                        if (customerProductPricing.Price3 != null) { customerProductPricing.Price3 = Math.Round((decimal)customerProductPricing.Price3, Globals.SOLUTION_NUM_DECIMAL_PLACES); }
                        customerProductPricing.Price5 = pricing5?.Rate == null ? null : customerProductPricing.PriceBase * (decimal?)(1 - pricing5.Rate / 100);

                        if (customerProductPricing.Price5 != null) { customerProductPricing.Price5 = Math.Round((decimal)customerProductPricing.Price5, Globals.SOLUTION_NUM_DECIMAL_PLACES); }

                        customerProductPricing.Price7 = pricing7?.Rate == null ? null : customerProductPricing.PriceBase * (decimal?)(1 - pricing7.Rate / 100);
                        if (customerProductPricing.Price7 != null) { customerProductPricing.Price7 = Math.Round((decimal)customerProductPricing.Price7, Globals.SOLUTION_NUM_DECIMAL_PLACES); }

                        customerProductPricing.Price9 = pricing9?.Rate == null ? null : customerProductPricing.PriceBase * (decimal?)(1 - pricing9.Rate / 100);
                        if (customerProductPricing.Price9 != null) { customerProductPricing.Price9 = Math.Round((decimal)customerProductPricing.Price9, Globals.SOLUTION_NUM_DECIMAL_PLACES); }

                        customerProductPricing.Price11 = pricing11?.Rate == null ? null : customerProductPricing.PriceBase * (decimal?)(1 - pricing11.Rate / 100);
                        if (customerProductPricing.Price11 != null) { customerProductPricing.Price11 = Math.Round((decimal)customerProductPricing.Price11, Globals.SOLUTION_NUM_DECIMAL_PLACES); }
                    }

                    csv += customerProductPricing.ExportToCSVRow(customer, product);
                }
            }

            //File.WriteAllText("../../../output.csv", csv);
            Console.WriteLine("Finished Generating Pricing Data");

            //return new HttpResponseMessage(HttpStatusCode.Created);

            byte[] filebytes = Encoding.UTF8.GetBytes(csv);
            return new FileContentResult(filebytes, "application/octet-stream") { FileDownloadName = "Output.csv" };
        }
    }


    public static class DBFunctions
    {
        public static List<Customer> FetchCustomers(string? customerID)
        {
            string dbConnectionString = Globals.DB_CONNECTION_STRING;
            List<Customer> customers = new();

            const int INDEX_ID = 0;
            const int INDEX_PBOOK = 1;
            const int INDEX_BCODE = 2;

            string commandText;

            if (customerID == null)
            {
                commandText = $@"SELECT [ID], [PBOOK], [BCODE]
                                     FROM [stage].[Macsim_Opmetrix_Customers]";
            }
            else
            {
                commandText = $@"SELECT [ID], [PBOOK], [BCODE]
                                     FROM [stage].[Macsim_Opmetrix_Customers]
                                     WHERE [ID] = '{customerID}'";
            }

            SqlConnection connection = new(dbConnectionString);
            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = commandText;
            command.CommandTimeout = 0;
            SqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                if (reader.IsDBNull(INDEX_ID)) { continue; }
                if (reader.IsDBNull(INDEX_PBOOK)) { continue; }
                if (reader.IsDBNull(INDEX_BCODE)) { continue; }

                string id = reader.GetString(INDEX_ID);
                string pbook = reader.GetString(INDEX_PBOOK);
                string bcode = reader.GetString(INDEX_BCODE);

                customers.Add(new(id, pbook, bcode));
            }
            reader.Close();
            command.Dispose();
            connection.Close();
            return customers;
        }

        public static List<Product> FetchProducts(string? productID)
        {
            string dbConnectionString = Globals.DB_CONNECTION_STRING;
            List<Product> products = new();

            const int INDEX_PCODE = 0;
            const int INDEX_GROUP = 1;
            const int INDEX_SELL1 = 2;
            const int INDEX_SELL2 = 3;
            const int INDEX_SELL3 = 4;
            const int INDEX_SELL4 = 5;
            const int INDEX_SELL5 = 6;
            const int INDEX_SELL6 = 7;
            const int INDEX_SELL7 = 8;
            const int INDEX_SELL8 = 9;


            string commandText;
            if (productID == null)
            {
                commandText = $@"SELECT [PCODE], [GROUP], [SELL1], [SELL2], [SELL3], [SELL4], [SELL5], [SELL6], [SELL7], [SELL8]
                                     FROM [stage].[Macsim_Opmetrix_Products]";
            }
            else
            {
                commandText = $@"SELECT [PCODE], [GROUP], [SELL1], [SELL2], [SELL3], [SELL4], [SELL5], [SELL6], [SELL7], [SELL8]
                                    FROM [stage].[Macsim_Opmetrix_Products]
                                    WHERE [PCODE] = '{productID}'";
            }

            SqlConnection connection = new(dbConnectionString);
            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = commandText;
            command.CommandTimeout = 0;
            SqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                if (reader.IsDBNull(INDEX_PCODE)) { continue; }
                if (reader.IsDBNull(INDEX_GROUP)) { continue; }

                string pcode = reader.GetString(INDEX_PCODE);
                string group = reader.GetString(INDEX_GROUP);
                double sell1 = reader.IsDBNull(INDEX_SELL1) ? 0 : reader.GetDouble(INDEX_SELL1);
                double sell2 = reader.IsDBNull(INDEX_SELL2) ? 0 : reader.GetDouble(INDEX_SELL2);
                double sell3 = reader.IsDBNull(INDEX_SELL3) ? 0 : reader.GetDouble(INDEX_SELL3);
                double sell4 = reader.IsDBNull(INDEX_SELL4) ? 0 : reader.GetDouble(INDEX_SELL4);
                double sell5 = reader.IsDBNull(INDEX_SELL5) ? 0 : reader.GetDouble(INDEX_SELL5);
                double sell6 = reader.IsDBNull(INDEX_SELL6) ? 0 : reader.GetDouble(INDEX_SELL6);
                double sell7 = reader.IsDBNull(INDEX_SELL7) ? 0 : reader.GetDouble(INDEX_SELL7);
                double sell8 = reader.IsDBNull(INDEX_SELL8) ? 0 : reader.GetDouble(INDEX_SELL8);

                products.Add(new(pcode, group, sell1, sell2, sell3, sell4, sell5, sell6, sell7, sell8));
            }
            reader.Close();
            command.Dispose();
            connection.Close();
            return products;
        }

        public static Pricing? FetchPricing(string stock, string type, string? account)
        {
            string dbConnectionString = Globals.DB_CONNECTION_STRING;

            const int INDEX_STOCK = 0;
            const int INDEX_ACCOUNT = 1;
            const int INDEX_RATE = 2;
            const int INDEX_TYPE = 3;

            string commandText;

            if (account == null)
            {
                commandText = $@"SELECT [stock], [account], [rate], [type]
                                    FROM [stage].[Macsim_Opmetrix_ContractPricing]
                                    WHERE [stock] = '{stock}' AND [account] is null";
            }
            else
            {
                commandText = $@"SELECT [stock], [account], [rate], [type]
                                    FROM [stage].[Macsim_Opmetrix_ContractPricing]
                                    WHERE [stock] = '{stock}' AND [account] = '{account}' AND [type] = '{type}'";
            }

            SqlConnection connection = new(dbConnectionString);
            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = commandText;
            command.CommandTimeout = 0;
            SqlDataReader reader = command.ExecuteReader();

            Pricing? pricing = null;

            // Should be either 1 or 0 pricing rows
            while (reader.Read())
            {
                if (reader.IsDBNull(INDEX_STOCK)) { continue; }
                if (reader.IsDBNull(INDEX_RATE)) { continue; }
                if (reader.IsDBNull(INDEX_TYPE)) { continue; }

                string _stock = reader.GetString(INDEX_STOCK);
                string? _account = reader.IsDBNull(INDEX_ACCOUNT) ? null : reader.GetString(INDEX_ACCOUNT);
                double rate = reader.GetDouble(INDEX_RATE);
                int _type = reader.GetInt32(INDEX_TYPE);

                pricing = new(_stock, rate, _type, _account);
            }
            reader.Close();
            command.Dispose();
            connection.Close();
            return pricing;
        }

        public static List<Pricing> FetchPricingGroup(string? account)
        {
            string dbConnectionString = Globals.DB_CONNECTION_STRING;

            const int INDEX_STOCK = 0;
            const int INDEX_ACCOUNT = 1;
            const int INDEX_RATE = 2;
            const int INDEX_TYPE = 3;

            string commandText;

            if (account == null)
            {
                commandText = $@"SELECT [stock], [account], [rate], [type]
                                    FROM [stage].[Macsim_Opmetrix_ContractPricing]
                                    WHERE [account] IS null";
            }
            else
            {
                commandText = $@"SELECT [stock], [account], [rate], [type]
                                    FROM [stage].[Macsim_Opmetrix_ContractPricing]
                                    WHERE [account] = '{account}'";
            }

            SqlConnection connection = new(dbConnectionString);
            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = commandText;
            command.CommandTimeout = 0;
            SqlDataReader reader = command.ExecuteReader();

            List<Pricing> pricings = new();

            // Should be either 1 or 0 pricing rows
            while (reader.Read())
            {
                if (reader.IsDBNull(INDEX_STOCK)) { continue; }
                if (reader.IsDBNull(INDEX_RATE)) { continue; }
                if (reader.IsDBNull(INDEX_TYPE)) { continue; }

                string _stock = reader.GetString(INDEX_STOCK);
                string? _account = reader.IsDBNull(INDEX_ACCOUNT) ? null : reader.GetString(INDEX_ACCOUNT);
                double rate = reader.GetDouble(INDEX_RATE);
                int _type = reader.GetInt32(INDEX_TYPE);

                pricings.Add(new(_stock, rate, _type, _account));
            }
            reader.Close();
            command.Dispose();
            connection.Close();


            if (pricings.Count > 1)
            {
                Console.WriteLine("Test Me");
            }

            return pricings;
        }
    }



    public static class Output
    {
        public static void SaveOutputToCSV(string fileName = "output")
        {
            FileStream filestream = new FileStream($"../../../{fileName}.csv", FileMode.Create);
            var streamwriter = new StreamWriter(filestream);
            streamwriter.AutoFlush = true;
            Console.SetOut(streamwriter);
            Console.SetError(streamwriter);
        }
    }


    public class Customer
    {
        public string ID { get; set; }
        public string PBook { get; set; }
        public string BCode { get; set; }

        public Customer(string id, string pbook, string bcode)
        {
            ID = id;
            PBook = pbook;
            BCode = bcode;
        }
    }

    public class Product
    {
        public string PCode { get; set; }
        public string Group { get; set; }
        public double Sell1 { get; set; }
        public double Sell2 { get; set; }
        public double Sell3 { get; set; }
        public double Sell4 { get; set; }
        public double Sell5 { get; set; }
        public double Sell6 { get; set; }
        public double Sell7 { get; set; }
        public double Sell8 { get; set; }

        public Product(string pcode, string group, double sell1, double sell2, double sell3, double sell4, double sell5, double sell6, double sell7, double sell8)
        {
            PCode = pcode;
            Group = group;
            Sell1 = sell1;
            Sell2 = sell2;
            Sell3 = sell3;
            Sell4 = sell4;
            Sell5 = sell5;
            Sell6 = sell6;
            Sell7 = sell7;
            Sell8 = sell8;
        }
    }

    public class Pricing
    {
        public string Stock { get; set; }
        public string? Account { get; set; }
        public double Rate { get; set; }
        public int Type { get; set; }


        public Pricing(string stock, double rate, int type, string? account = null)
        {
            Stock = stock;
            Rate = rate;
            Type = type;
            Account = account;
        }
    }

    public class CustomerProductPricing
    {
        public string CustomerID { get; set; }
        public string BCode { get; set; }
        public string ProductID { get; set; }
        public string ProductGroup { get; set; }
        public decimal? PriceBase { get; set; }
        public decimal? Price1 { get; set; }
        public decimal? Price2 { get; set; }
        public decimal? Price3 { get; set; }
        public decimal? Price4 { get; set; }
        public decimal? Price5 { get; set; }
        public decimal? Price6 { get; set; }
        public decimal? Price7 { get; set; }
        public decimal? Price8 { get; set; }
        public decimal? Price9 { get; set; }
        public decimal? Price10 { get; set; }
        public decimal? Price11 { get; set; }
        public decimal? Price12 { get; set; }

        public CustomerProductPricing(string customerID, string bCode, string productID, string productGroup)
        {
            CustomerID = customerID;
            BCode = bCode;
            ProductID = productID;
            ProductGroup = productGroup;
        }

        // Modified to included PBOOK from customers
        public static string GenerateCSVHeader()
        {
            return "CustomerID, BCode, PBook, ProductID, ProductGroup, Sell1, Sell2, Sell3, Sell4, Sell5, Sell6, Sell7, Sell8, PriceBase, Price1, Price2, Price3, Price4, Price5, Price6, Price7, Price8, Price9, Price10, Price11, Price12\n";
        }

        public string ExportToCSVRow(Customer customer, Product product)
        {
            return $"{CustomerID}, {BCode}, {customer.PBook}, {ProductID}, {ProductGroup}, {product.Sell1}, {product.Sell2}, {product.Sell3}, {product.Sell4}, {product.Sell5}, {product.Sell6}, {product.Sell7}, {product.Sell8}, {PriceBase}, {Price1}, {Price2}, {Price3}, {Price4}, {Price5}, {Price6}, {Price7}, {Price8}, {Price9}, {Price10}, {Price11}, {Price12}\n";
        }
    }

    public static class Globals
    {
        public const string DEFAULT_USER_ID = "Azure";
        public const string DB_CONNECTION_STRING = @$"Server=primarius-prod.database.windows.net,1433; 
                                                      Database=sp_prod1; 
                                                      User id=primarius@primarius-prod; 
                                                      Password=pr1mar1u$;";
        public const int TYPE_OVERRIDE = 1;
        public const int TYPE_DISCOUNT = 2;
        public const int SOLUTION_NUM_DECIMAL_PLACES = 3;
    }

}
