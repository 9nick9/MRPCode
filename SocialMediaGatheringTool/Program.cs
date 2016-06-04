using Bing;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SocialMediaGatheringTool
{
	class Program
	{

		static void Main(string[] args)
		{
			switch (args[0])
			{
				case "Load500":
					LoadFortune500(args[1], args[2], int.Parse(args[3]));
					break;
				case "SearchForWebsites":
					FindWebsites(args[1], args[2]);
					break;
				case "LoadKred":
					LoadKred(args[1]);
					break;
				default:
					Console.Out.WriteLine("Please enter a proper command");
					return;
			}			
		}

		static void FindWebsites(string connectionString, string accountKey)
		{
			using (SqlConnection connection = new SqlConnection(connectionString))
			{
				connection.Open();
				SqlCommand cmd = new SqlCommand();
				cmd.Connection = connection;

				BingSearchContainer container = new BingSearchContainer(new Uri("https://api.datamarket.azure.com/Data.ashx/Bing/Search"));
				container.Credentials = new NetworkCredential(accountKey, accountKey);

				string getCompaniesFromDB = "SELECT ID, Name from Company";
				cmd.CommandText = getCompaniesFromDB;

				SqlDataReader reader = cmd.ExecuteReader();
				Dictionary<string, int> nameIDDictionary = new Dictionary<string, int>();

				while (reader.Read())
					nameIDDictionary.Add((string) reader["Name"], (int) reader["ID"]);

				reader.Close();

				foreach (KeyValuePair<string, int> kvp in nameIDDictionary)
				{
					string url = GetWebsiteFromBing(kvp.Key, container);
					Console.Out.WriteLine($"The URL for {kvp.Key} is {url}");

					string updateWebsiteSQL = $"UPDATE Company SET Website = '{url}' Where ID = {kvp.Value}";
					cmd.CommandText = updateWebsiteSQL;

					cmd.ExecuteNonQuery();
				}
			}
		}

		static string GetWebsiteFromBing(string company, BingSearchContainer bingSearch)
		{
			DataServiceQuery<WebResult> result = bingSearch.Web(company, "DisableLocationDetection", null, "en-US", null, null, null, null);
			WebResult firstResult = result.FirstOrDefault();

			if (firstResult != null)
				return firstResult.Url;

			return "";
		}

		static void LoadFortune500(string connectionString, string fileLocation, int resetDB = 0)
		{
			string jsonText = File.ReadAllText(fileLocation);
			var jsonResult = JsonConvert.DeserializeObject(jsonText);
			JObject jsonObject = jsonResult as JObject;
			if (jsonObject == null)
				throw new ArgumentException();

			JToken companies = jsonObject["companies"];

			using (SqlConnection connection = new SqlConnection(connectionString))
			{
				int count = 0;
				connection.Open();
				if (resetDB == 1)
					ResetDB(connection);

				foreach (JToken company in companies.Children())
				{
					//Get company info
					string name = company.Value<string>("title");
					string fortuneProfileLink = company.Value<string>("permalink");

					JToken categories = company.Value<JToken>("filter");
					string industry = categories.Value<string>("industry");
					string hqCity = categories.Value<string>("hqcity");
					string hqState = categories.Value<string>("hqstate");
					string ceoFounder = categories.Value<string>("ceofounder");
					string ceoForeign = categories.Value<string>("ceoforeign");
					string ceowoman = categories.Value<string>("ceowoman");
					string sector = categories.Value<string>("sector");

					JToken stats = company.Value<JToken>("sort");
					int profits = 0;
					JValue prof = stats.Value<JValue>("profits");
					if(prof.Type == JTokenType.Integer)
						profits = stats.Value<int>("profits");

					int assests = stats.Value<int>("assets");
					int employees = stats.Value<int>("employees");
					JValue mktv = stats.Value<JValue>("mktval");

					int marketValue = 0;
					if (mktv.Type == JTokenType.Integer)
						marketValue = stats.Value<int>("mktval"); ;

					JArray subsidiaries = company.Value<JArray>("companies");
					JToken stockInfo = subsidiaries[0].Value<JToken>("ticker");

					string stockSymbol = "";
					if (stockInfo.Children().Count() > 0)
						stockSymbol = stockInfo.Value<string>("symbol");

					int foreign = ceoForeign == "no" ? 0 : 1;
					int woman = ceowoman == "no" ? 0 : 1;
					int founder = ceoFounder == "no" ? 0 : 1;

					SqlCommand cmd = new SqlCommand();
					SqlTransaction transaction = connection.BeginTransaction("SqlTransaction");
					cmd.Transaction = transaction;
					cmd.Connection = connection;

					try
					{
						//Insert into company table
						string insertCompany = $"INSERT INTO Company (Name, Industry, Sector, City, State, FemaleCEO, ForeignCEO, FounderCEO, StockTicker) VALUES('{name}', '{industry}', '{sector}', '{hqCity}', '{hqState}', '{woman}', '{foreign}', '{founder}', '{stockSymbol}')";
						cmd.CommandText = insertCompany;
						cmd.ExecuteNonQuery();

						//Get ID
						string getIdentity = "Select SCOPE_IDENTITY()";
						cmd.CommandText = getIdentity;
						int companyID = Convert.ToInt32(cmd.ExecuteScalar());

						//Insert into stats table
						string insertCompanyStats = $"INSERT INTO CompanyStats (CompanyID, Profits, Assets, Employees, MarketValue) VALUES({companyID}, {profits}, {assests}, {employees}, {marketValue})";
						cmd.CommandText = insertCompanyStats;
						cmd.ExecuteNonQuery();

						//Get website info
						//string website = GetWebsite(fortuneProfileLink); 
						
						////insert website
						//string insertWebsite = $"UPDATE Company SET Website = '{website}' Where ID = {companyID}";
						//cmd.CommandText = insertWebsite;
						//cmd.ExecuteNonQuery();

						transaction.Commit();

						Console.Out.WriteLine($"Currently @ {name} which is count {++count}");
						//Do not hammer Fortune website and get blocked
						//Thread.Sleep(10000);
					}
					catch (SqlException e)
					{
						transaction.Rollback();
						Console.Out.Write($"There was an error with {name}. Error Message: {e.Message}");
						break;
					}					
				}
				connection.Close();
			}
		}

		static void ResetDB(SqlConnection connection)
		{
			string deleteStats = "DELETE FROM CompanyStats";
			string deleteCompany = "DELETE FROM Company";
			string resetIdent = "DBCC CHECKIDENT (Company, reseed, 0)";

			SqlCommand cmd = new SqlCommand();
			cmd.Connection = connection;

			cmd.CommandText = deleteStats;
			cmd.ExecuteNonQuery();

			cmd.CommandText = deleteCompany;
			cmd.ExecuteNonQuery();

			cmd.CommandText = resetIdent;
			cmd.ExecuteNonQuery();
		}

		static string GetWebsite(string fortuneProfileLink, int retryCount = 5)
		{
			WebRequest request = WebRequest.Create(fortuneProfileLink);
			try
			{
				request.Timeout = 45000;
				WebResponse response = request.GetResponse();
				Stream dataStream = response.GetResponseStream();
				StreamReader reader = new StreamReader(dataStream);
				while (!reader.EndOfStream)
				{
					string curLine = reader.ReadLine();

					if (!curLine.Contains("<th>Website</th>"))
						continue;

					reader.ReadLine();
					reader.ReadLine();
					string websiteLine = reader.ReadLine();
					int stringStart = websiteLine.IndexOf(">") + 1;
					int stringEnd = websiteLine.IndexOf("<", stringStart);
					return websiteLine.Substring(stringStart, stringEnd - stringStart);
				}
			}
			catch(WebException)
			{
				//if (retryCount > 0)
				//	return GetWebsite(fortuneProfileLink, --retryCount);
				//else
				//{
					Console.Out.WriteLine($"The link {fortuneProfileLink} has timed out.");
					return "";
				//}
			}
			return "";
		}

		static void LoadKred(string connectionString)
		{
			using (SqlConnection connection = new SqlConnection(connectionString))
			{

			}
		}
	}
}
