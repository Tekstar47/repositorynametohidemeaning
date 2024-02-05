#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Data;
using System.Collections.Generic;
using System.Text;
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
            bool debug = !string.IsNullOrEmpty(req.Query["debug"]);

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
            if (debug)
            {
                csv = CustomerProductPricingFullInformation.GenerateCSVHeader();
            }

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

                List<CustomerProductPricingBase> customerProductPricings = new();

                //List<CustomerProductPricing> customerProductPricings = new();
                //List<CustomerProductPricingFullInformation> customerProductPricingsFullInformation = new();

                foreach (var product in products)
                {
                    // If Barcode is null, the item is discontinued
                    if (!debug && product.BarCode == null)
                    {
                        continue;
                    }

                    // If a stock category starts with ZZ, then the product is DELETED/CANCELLED - Steve Hermosilla from Macsim 2023/12/05 in email chain
                    if (!debug && product.PCode.StartsWith("ZZ")) {
                        continue;
                    }

                    int x = 0;
                    Int32.TryParse(product.Group, out x);

                    //// If Product Group is a two digit number, it is a Eclipse item and we won't be including it
                    //if (!debug && !Int32.TryParse(product.Group, out x))
                    //{
                    //    continue;
                    //}

                    Decimal BasePrice;

                    // Set base price
                    switch (customer.PBook)
                    {
                        case "1":
                            BasePrice = (decimal)product.Sell1;
                            break;
                        case "2":
                            BasePrice = (decimal)product.Sell2;
                            break;
                        case "3":
                            BasePrice = (decimal)product.Sell3;
                            break;
                        case "4":
                            BasePrice = (decimal)product.Sell4;
                            break;
                        case "5":
                            BasePrice = (decimal)product.Sell5;
                            break;
                        case "6":
                            BasePrice = (decimal)product.Sell6;
                            break;
                        case "7":
                            BasePrice = (decimal)product.Sell7;
                            break;
                        case "8":
                            BasePrice = (decimal)product.Sell8;
                            break;
                        default:
                            throw new Exception("Invalid PBook value");
                    }

                    // Any item with a base price of 0 is a discontinued item and can be ignored
                    if (!debug && BasePrice == 0)
                    {
                        continue;
                    }

                    var pricing1 = pricingsGeneral.FindAll(x => x.Stock == product.Group && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing2 = pricingsGeneral.FindAll(x => x.Stock == product.Group && x.Type == Globals.TYPE_OVERRIDE);

                    var pricing3 = pricingsGeneral.FindAll(x => x.Stock == product.PCode && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing4 = pricingsGeneral.FindAll(x => x.Stock == product.PCode && x.Type == Globals.TYPE_OVERRIDE);


                    var pricing5 = pricingCustomerType.FindAll(x => x.Stock == product.Group && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing6 = pricingCustomerType.FindAll(x => x.Stock == product.Group && x.Type == Globals.TYPE_OVERRIDE);

                    var pricing7 = pricingCustomerType.FindAll(x => x.Stock == product.PCode && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing8 = pricingCustomerType.FindAll(x => x.Stock == product.PCode && x.Type == Globals.TYPE_OVERRIDE);


                    var pricing9 = pricingCustomerSpecific.FindAll(x => x.Stock == product.Group && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing10 = pricingCustomerSpecific.FindAll(x => x.Stock == product.Group && x.Type == Globals.TYPE_OVERRIDE);

                    var pricing11 = pricingCustomerSpecific.FindAll(x => x.Stock == product.PCode && x.Type == Globals.TYPE_DISCOUNT);
                    var pricing12 = pricingCustomerSpecific.FindAll(x => x.Stock == product.PCode && x.Type == Globals.TYPE_OVERRIDE);


                    if (debug)
                    {
                        CustomerProductPricingFullInformation customerProductPricingFullInformation =
    new CustomerProductPricingFullInformation(customer.ID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, product);

                        customerProductPricingFullInformation.PriceBase = BasePrice;
                        // Set all other prices

                        // Set all Price Overrides (type = 1)
                        {
                            if (pricing2?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price2 = Math.Round((decimal)pricing2[0].Rate, Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks2 = pricing2.Count;
                            }

                            if (pricing4?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price4 = Math.Round((decimal)pricing4[0].Rate, Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks4 = pricing4.Count;
                            }

                            if (pricing6?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price6 = Math.Round((decimal)pricing6[0].Rate, Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks6 = pricing6.Count;
                            }

                            if (pricing8?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price8 = Math.Round((decimal)pricing8[0].Rate, Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks8 = pricing8.Count;
                            }

                            if (pricing10?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price10 = Math.Round((decimal)pricing10[0].Rate, Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks10 = pricing10.Count;
                            }

                            if (pricing12?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price12 = Math.Round((decimal)pricing12[0].Rate, Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks12 = pricing12.Count;
                            }
                        }

                        // Set all discounts (type = 2)
                        {
                            if (pricing1?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price1 = Math.Round((decimal)(customerProductPricingFullInformation.PriceBase * (decimal?)(1 - pricing1[0].Rate / 100)), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks1 = pricing1.Count;
                            }

                            if (pricing3?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price3 = Math.Round((decimal)(customerProductPricingFullInformation.PriceBase * (decimal?)(1 - pricing3[0].Rate / 100)), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks3 = pricing3.Count;
                            }

                            if (pricing5?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price5 = Math.Round((decimal)(customerProductPricingFullInformation.PriceBase * (decimal?)(1 - pricing5[0].Rate / 100)), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks5 = pricing5.Count;
                            }

                            if (pricing7?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price7 = Math.Round((decimal)(customerProductPricingFullInformation.PriceBase * (decimal?)(1 - pricing7[0].Rate / 100)), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks7 = pricing7.Count;
                            }

                            if (pricing9?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price9 = Math.Round((decimal)(customerProductPricingFullInformation.PriceBase * (decimal?)(1 - pricing9[0].Rate / 100)), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks9 = pricing9.Count;
                            }

                            if (pricing11?.Count > 0)
                            {
                                customerProductPricingFullInformation.Price11 = Math.Round((decimal)(customerProductPricingFullInformation.PriceBase * (decimal?)(1 - pricing11[0].Rate / 100)), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                                customerProductPricingFullInformation.QtyBreaks11 = pricing11.Count;
                            }
                        }

                        // If there is no special customer pricing
                        if (customerProductPricingFullInformation.Price1 == null && customerProductPricingFullInformation.Price2 == null && customerProductPricingFullInformation.Price3 == null && customerProductPricingFullInformation.Price4 == null
                            && customerProductPricingFullInformation.Price5 == null && customerProductPricingFullInformation.Price6 == null && customerProductPricingFullInformation.Price7 == null && customerProductPricingFullInformation.Price8 == null
                            && customerProductPricingFullInformation.Price9 == null && customerProductPricingFullInformation.Price10 == null && customerProductPricingFullInformation.Price11 == null && customerProductPricingFullInformation.Price12 == null)
                        {
                            continue;
                        }

                        customerProductPricings.Add(customerProductPricingFullInformation);
                        continue;
                    }

                    // Calcaulte all pricings

                    // Otherwise filter to just the highest priority customer price
                    // Check from pricing 12 all the way down

                    // Jeremy 2023/11/16 After the price files were reviewed, it seems that pricing 8 should override both pricing 9 and pricing 12. I have placed pricing 8 as the highest priority for now. Will try to get clarification on the pricing priority order.

                    if (pricing12.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing12, BasePrice));
                        continue;
                    }

                    if (pricing11.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing11, BasePrice));
                        continue;
                    }

                    if (pricing10.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing10, BasePrice));
                        continue;
                    }

                    if (pricing9.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing9, BasePrice));
                        continue;
                    }

                    if (pricing8.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing8, BasePrice));
                        continue;
                    }

                    if (pricing7.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing7, BasePrice));
                        continue;
                    }

                    if (pricing6.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing6, BasePrice));
                        continue;
                    }

                    if (pricing5.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing5, BasePrice));
                        continue;
                    }

                    if (pricing4.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing4, BasePrice));
                        continue;
                    }

                    if (pricing3.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing3, BasePrice));
                        continue;
                    }

                    if (pricing2.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing2, BasePrice));
                        continue;
                    }

                    if (pricing1.Count > 0)
                    {
                        customerProductPricings.Add(new CustomerProductPricing(customerID, customer.BCode, customer.PBook, product.PCode, product.Group, product.BarCode, pricing1, BasePrice));
                        continue;
                    }
                }

                foreach (var customerProductPricing in customerProductPricings)
                {
                    csv += customerProductPricing.ExportToCSVRow();
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
            } else
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
            const int INDEX_BARCODE = 10;
            const int INDEX_COST = 11;


            string commandText;
            if (productID == null)
            {
                commandText = $@"SELECT [PCODE], [GROUP], [SELL1], [SELL2], [SELL3], [SELL4], [SELL5], [SELL6], [SELL7], [SELL8], [BARCODE], [COST]
                                     FROM [stage].[Macsim_Opmetrix_Products]";
            } else
            {
                commandText = $@"SELECT [PCODE], [GROUP], [SELL1], [SELL2], [SELL3], [SELL4], [SELL5], [SELL6], [SELL7], [SELL8], [BARCODE], [COST]
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
                string barcode = reader.IsDBNull(INDEX_BARCODE) ? "" : reader.GetString(INDEX_BARCODE);
                double sell1 = reader.IsDBNull(INDEX_SELL1) ? 0 : reader.GetDouble(INDEX_SELL1);
                double sell2 = reader.IsDBNull(INDEX_SELL2) ? 0 : reader.GetDouble(INDEX_SELL2);
                double sell3 = reader.IsDBNull(INDEX_SELL3) ? 0 : reader.GetDouble(INDEX_SELL3);
                double sell4 = reader.IsDBNull(INDEX_SELL4) ? 0 : reader.GetDouble(INDEX_SELL4);
                double sell5 = reader.IsDBNull(INDEX_SELL5) ? 0 : reader.GetDouble(INDEX_SELL5);
                double sell6 = reader.IsDBNull(INDEX_SELL6) ? 0 : reader.GetDouble(INDEX_SELL6);
                double sell7 = reader.IsDBNull(INDEX_SELL7) ? 0 : reader.GetDouble(INDEX_SELL7);
                double sell8 = reader.IsDBNull(INDEX_SELL8) ? 0 : reader.GetDouble(INDEX_SELL8);
                double? cost = reader.IsDBNull(INDEX_COST) ? null : reader.GetDouble(INDEX_COST);


                products.Add(new(pcode, group, barcode, sell1, sell2, sell3, sell4, sell5, sell6, sell7, sell8, cost));
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
            const int INDEX_QTY = 4;

            string commandText;

            if (account == null)
            {
                commandText = $@"SELECT [stock], [account], [rate], [type], [quantity]
                                    FROM [stage].[Macsim_Opmetrix_ContractPricing]
                                    WHERE [stock] = '{stock}' AND [account] is null";
            } else
            {
                commandText = $@"SELECT [stock], [account], [rate], [type], [quantity]
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
                int qty = reader.IsDBNull(INDEX_QTY) ? 0 : (int)reader.GetDouble(INDEX_QTY);

                pricing = new(_stock, rate, _type, qty, _account);
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
            const int INDEX_QTY = 4;

            string commandText;

            if (account == null)
            {
                commandText = $@"SELECT [stock], [account], [rate], [type], [quantity]
                                    FROM [stage].[Macsim_Opmetrix_ContractPricing]
                                    WHERE [account] IS null";
            } else
            {
                commandText = $@"SELECT [stock], [account], [rate], [type], [quantity]
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
                int qty = reader.IsDBNull(INDEX_QTY) ? 0 : (int)reader.GetDouble(INDEX_QTY);

                pricings.Add(new(_stock, rate, _type, qty, _account));
            }
            reader.Close();
            command.Dispose();
            connection.Close();

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
        public string BarCode { get; set; }
        public double Sell1 { get; set; }
        public double Sell2 { get; set; }
        public double Sell3 { get; set; }
        public double Sell4 { get; set; }
        public double Sell5 { get; set; }
        public double Sell6 { get; set; }
        public double Sell7 { get; set; }
        public double Sell8 { get; set; }
        public double? Cost { get; set; } // Not really important but including for debug reasons

        public Product(string pcode, string group, string barcode, double sell1, double sell2, double sell3, double sell4, double sell5, double sell6, double sell7, double sell8, double? cost = null)
        {
            PCode = pcode;
            Group = group;
            BarCode = barcode;
            Sell1 = sell1;
            Sell2 = sell2;
            Sell3 = sell3;
            Sell4 = sell4;
            Sell5 = sell5;
            Sell6 = sell6;
            Sell7 = sell7;
            Sell8 = sell8;
            Cost = cost;
        }
    }

    public class Pricing
    {
        public string Stock { get; set; }
        public string? Account { get; set; }
        public double Rate { get; set; }
        public int Type { get; set; }
        public int Qty { get; set; }


        public Pricing(string stock, double rate, int type, int qty, string? account = null)
        {
            Stock = stock;
            Rate = rate;
            Type = type;
            Qty = qty;
            Account = account;
        }
    }

    public class CustomerProductPricingBase
    {
        public string CustomerID { get; set; }
        public string BCode { get; set; }
        public string PBook { get; set; }
        public string ProductID { get; set; }
        public string ProductGroup { get; set; }
        public string Barcode { get; set; }

        public CustomerProductPricingBase(string customerID, string bCode, string pBook, string productID, string productGroup, string barcode)
        {

            CustomerID = customerID;
            BCode = bCode;
            PBook = pBook;
            ProductID = productID;
            ProductGroup = productGroup;
            Barcode = barcode;
        }

        public virtual string ExportToCSVRow() { return ""; }
    }

    public class CustomerProductPricingFullInformation : CustomerProductPricingBase
    {
        public Product _Product { get; set; }
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
        public int QtyBreaks1 { get; set; }
        public int QtyBreaks2 { get; set; }
        public int QtyBreaks3 { get; set; }
        public int QtyBreaks4 { get; set; }
        public int QtyBreaks5 { get; set; }
        public int QtyBreaks6 { get; set; }
        public int QtyBreaks7 { get; set; }
        public int QtyBreaks8 { get; set; }
        public int QtyBreaks9 { get; set; }
        public int QtyBreaks10 { get; set; }
        public int QtyBreaks11 { get; set; }
        public int QtyBreaks12 { get; set; }

        public CustomerProductPricingFullInformation(string customerID, string bCode, string pBook, string productID, string productGroup, string barcode, Product product) : base(customerID, bCode, pBook, productID, productGroup, barcode)
        {
            _Product = product;
            QtyBreaks1 = 0;
            QtyBreaks2 = 0;
            QtyBreaks3 = 0;
            QtyBreaks4 = 0;
            QtyBreaks5 = 0;
            QtyBreaks6 = 0;
            QtyBreaks7 = 0;
            QtyBreaks8 = 0;
            QtyBreaks9 = 0;
            QtyBreaks10 = 0;
            QtyBreaks11 = 0;
            QtyBreaks12 = 0;
        }

        public static string GenerateCSVHeader()
        {
            return "CustomerID, BCode, PBook, ProductID, ProductGroup, Barcode, Sell1, Sell2, Sell3, Sell4, Sell5, Sell6, Sell7, Sell8, Cost, PriceBase, Price1, Qty1, Price2, Qty2, Price3, Qty3, Price4, Qty4, Price5, Qty5, Price6, Qty6, Price7, Qty7, Price8, Qty8, Price9, Qty9, Price10, Qty10, Price11, Qty11, Price12, Qty12\n";
        }

        public override string ExportToCSVRow()
        {
            return $"{CustomerID}, {BCode}, {PBook}, {ProductID}, {ProductGroup}, {Barcode}, {_Product.Sell1}, {_Product.Sell2}, {_Product.Sell3}, {_Product.Sell4}, {_Product.Sell5}, {_Product.Sell6}, {_Product.Sell7}, {_Product.Sell8}, {_Product.Cost}, {PriceBase}, {Price1}, {QtyBreaks1}, {Price2}, {QtyBreaks2}, {Price3}, {QtyBreaks3}, {Price4}, {QtyBreaks4}, {Price5}, {QtyBreaks5}, {Price6}, {QtyBreaks6}, {Price7}, {QtyBreaks7}, {Price8}, {QtyBreaks8}, {Price9}, {QtyBreaks9}, {Price10}, {QtyBreaks10}, {Price11}, {QtyBreaks11}, {Price12}, {QtyBreaks12}\n";
        }
    }

    public class CustomerProductPricing : CustomerProductPricingBase
    {
        public decimal Price { get; set; }
        public decimal? Price2 { get; set; }
        public decimal? Price3 { get; set; }
        public decimal? Price4 { get; set; }
        public decimal? Price5 { get; set; }
        public decimal? Price6 { get; set; }
        public decimal? Price7 { get; set; }
        public decimal? Price8 { get; set; }
        public decimal? Price9 { get; set; }
        public decimal? Price10 { get; set; }
        public int Qty { get; set; }
        public int? Qty2 { get; set; }
        public int? Qty3 { get; set; }
        public int? Qty4 { get; set; }
        public int? Qty5 { get; set; }
        public int? Qty6 { get; set; }
        public int? Qty7 { get; set; }
        public int? Qty8 { get; set; }
        public int? Qty9 { get; set; }
        public int? Qty10 { get; set; }

        public CustomerProductPricing(string customerID, string bCode, string pBook, string productID, string productGroup, string barcode, decimal price, int qty) : base(customerID, bCode, pBook, productID, productGroup, barcode)
        {
            Price = price;
            Qty = qty;
        }

        // Assuming sorted lowest qty to highest qty
        public CustomerProductPricing(string customerID, string bCode, string pBook, string productID, string productGroup, string barcode, List<Pricing> pricings, decimal basePrice) : base(customerID, bCode, pBook, productID, productGroup, barcode)
        {
            if (pricings.Count == 0)
            {
                throw new Exception("qty breakpoints found - the minimum is 1");
            }

            if (pricings.Count > 10)
            {
                throw new Exception("Too many qty breakpoints - the limit is 10");
            }

            switch(pricings[0].Type)
            {
                case Globals.TYPE_OVERRIDE:
                {
                    if (pricings.Count > 0)
                    {
                        Price = (decimal)pricings[0].Rate;
                        Qty = pricings[0].Qty;
                    }

                    if (pricings.Count > 1)
                    {
                        Price2 = (decimal)pricings[1].Rate;
                        Qty2 = pricings[1].Qty;
                    }

                    if (pricings.Count > 2)
                    {
                        Price3 = (decimal)pricings[2].Rate;
                        Qty3 = pricings[2].Qty;
                    }

                    if (pricings.Count > 3)
                    {
                        Price4 = (decimal)pricings[3].Rate;
                        Qty4 = pricings[3].Qty;
                    }

                    if (pricings.Count > 4)
                    {
                        Price5 = (decimal)pricings[4].Rate;
                        Qty5 = pricings[4].Qty;
                    }
                    if (pricings.Count > 5)
                    {
                        Price6 = (decimal)pricings[5].Rate;
                        Qty6 = pricings[5].Qty;
                    }
                    if (pricings.Count > 6)
                    {
                        Price7 = (decimal)pricings[6].Rate;
                        Qty7 = pricings[6].Qty;
                    }
                    if (pricings.Count > 7)
                    {
                        Price8 = (decimal)pricings[7].Rate;
                        Qty8 = pricings[7].Qty;
                    }
                    if (pricings.Count > 8)
                    {
                        Price9 = (decimal)pricings[8].Rate;
                        Qty9 = pricings[8].Qty;
                    }
                    if (pricings.Count > 9)
                    {
                        Price10 = (decimal)pricings[9].Rate;
                        Qty10 = pricings[9].Qty;
                    }
                    break;
                }
                case Globals.TYPE_DISCOUNT:
                {
                    if (pricings.Count > 0)
                    {
                        Price = Math.Round(basePrice * (decimal)(1 - pricings[0].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty = pricings[0].Qty;
                    }

                    if (pricings.Count > 1)
                    {
                        Price2 = Math.Round(basePrice * (decimal)(1 - pricings[1].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty2 = pricings[1].Qty;
                    }

                    if (pricings.Count > 2)
                    {
                        Price3 = Math.Round(basePrice * (decimal)(1 - pricings[2].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty3 = pricings[2].Qty;
                    }

                    if (pricings.Count > 3)
                    {
                        Price4 = Math.Round(basePrice * (decimal)(1 - pricings[3].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty4 = pricings[3].Qty;
                    }

                    if (pricings.Count > 4)
                    {
                        Price5 = Math.Round(basePrice * (decimal)(1 - pricings[4].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty5 = pricings[4].Qty;
                    }
                    if (pricings.Count > 5)
                    {
                        Price6 = Math.Round(basePrice * (decimal)(1 - pricings[5].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty6 = pricings[5].Qty;
                    }
                    if (pricings.Count > 6)
                    {
                        Price7 = Math.Round(basePrice * (decimal)(1 - pricings[6].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty7 = pricings[6].Qty;
                    }
                    if (pricings.Count > 7)
                    {
                        Price8 = Math.Round(basePrice * (decimal)(1 - pricings[7].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty8 = pricings[7].Qty;
                    }
                    if (pricings.Count > 8)
                    {
                        Price9 = Math.Round(basePrice * (decimal)(1 - pricings[8].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty9 = pricings[8].Qty;
                    }
                    if (pricings.Count > 9)
                    {
                        Price10 = Math.Round(basePrice * (decimal)(1 - pricings[9].Rate / 100), Globals.SOLUTION_NUM_DECIMAL_PLACES);
                        Qty10 = pricings[9].Qty;
                    }
                    break;
                }
                default:
                {
                    throw new Exception("Invalid pricing type");
                }
            }
        }

        public static string GenerateCSVHeader()
        {
            return GenerateFullCSVHeader();
        }

        public static string GenerateFullCSVHeader()
        {
            return "CustomerID, BCode, PBook, ProductID, ProductGroup, Barcode, Price, Qty, Price2, Qty2, Price3, Qty3, Price4, Qty4, Price5, Qty5, Price6, Qty6, Price7, Qty7, Price8, Qty8, Price9, Qty9, Price10, Qty10\n";
        }

        public override string ExportToCSVRow()
        {
            return ExportToCSVRowFull();
        }

        public string ExportToCSVRowFull()
        {
            return $"{CustomerID}, {BCode}, {PBook}, {ProductID}, {ProductGroup}, {Barcode}, {Price}, {Qty}, {Price2}, {Qty2}, {Price3}, {Qty3}, {Price4}, {Qty4}, {Price5}, {Qty5}, {Price6}, {Qty6}, {Price7}, {Qty7}, {Price8}, {Qty8}, {Price9}, {Qty9}, {Price10}, {Qty10}\n";
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
        public const int SOLUTION_NUM_DECIMAL_PLACES = 2;
    }

}
