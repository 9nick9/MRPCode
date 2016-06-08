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
using System.Xml;

namespace SocialMediaGatheringTool
{
	class Program
	{
		const string kloutBaseAddress = "http://api.klout.com/v2/";
		const string stockQuoteAddress = "http://dev.markitondemand.com/MODApis/Api/v2/Quote?symbol={0}";

		static void Main(string[] args)
		{
			switch (args[0])
			{
				case "Load500":
					LoadFortune500(args[1], args[2], int.Parse(args[3]));
					break;
				case "SearchForWebsites":
					FindWebsites(args[1], args[2], int.Parse(args[3]));
					break;
				case "LoadKlout":
					LoadKlout(args[1], args[2]);
					break;
				case "TestParsing":
					TestParse(args[1]);
					break;
				case "LoadStockData":
					LoadStockData(args[1]);
					break;
				default:
					Console.Out.WriteLine("Please enter a proper command");
					return;
			}			
		}

		static void LoadStockData(string connectionString)
		{
			using (SqlConnection connection = new SqlConnection(connectionString))
			{
				connection.Open();
				SqlCommand cmd = new SqlCommand();
				cmd.Connection = connection;

				//Get stock prices from today not yet added
				string getSymbols = "SELECT ID, StockTicker FROM Company c Left Join (select * from StockPrice where DateOfPrice = CONVERT(date, getdate())) s on c.ID = s.CompanyID Where StockTicker <> '' and StockTicker is not null and Price is null";
				cmd.CommandText = getSymbols;
				SqlDataReader reader = cmd.ExecuteReader();
				Dictionary<int, string> tickerDictionary = new Dictionary<int, string>();

				while (reader.Read())
					tickerDictionary.Add((int) reader["ID"], (string) reader["StockTicker"]);

				reader.Close();
				
				foreach(KeyValuePair<int, string> kvp in tickerDictionary)
				{
					string getStockData = string.Format(stockQuoteAddress, kvp.Value);
					Stream dataStream;
					try
					{
						WebRequest request = WebRequest.Create(getStockData);

						WebResponse response = request.GetResponse();
						dataStream = response.GetResponseStream();
					}
					catch
					{
						Thread.Sleep(1000);
						continue;
					}

					XmlDocument xml = new XmlDocument();
					xml.Load(dataStream);
					XmlNode price = xml.GetElementsByTagName("LastPrice")[0];
					if (price == null)
						continue;

					double lastPrice = double.Parse(price.InnerText);

					string insertStockPrice = $"INSERT INTO StockPrice VALUES({kvp.Key}, {lastPrice}, '{DateTime.Now.ToString("yyyy-MM-dd")}')";
					cmd.CommandText = insertStockPrice;
					cmd.ExecuteNonQuery();

					Console.Out.WriteLine($"Inserted Stock data for {kvp.Value}");
					Thread.Sleep(1000);
				}
			}
		}

		static void TestParse(string url)
		{
			WebResult result = new WebResult();
			result.Url = url;
			string handle = ParseOutTwitterHandle(result);
		}

		static void FindWebsites(string connectionString, string accountKey, int findTwitter)
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

				string column = "Website";
				if (findTwitter == 1)
					column = "Twitter";

				foreach (KeyValuePair<string, int> kvp in nameIDDictionary)
				{
					string url = SearchBing(kvp.Key, container, findTwitter == 1);
					Console.Out.WriteLine($"The value for {kvp.Key} is {url}");
					if (url.Length > 150)
						continue;

					string updateWebsiteSQL = $"UPDATE Company SET {column} = '{url}' Where ID = {kvp.Value}";
					cmd.CommandText = updateWebsiteSQL;

					cmd.ExecuteNonQuery();
				}
			}
		}

		static string SearchBing(string company, BingSearchContainer bingSearch, bool needTwitter = false)
		{
			if (needTwitter)
				company = "twitter: " + company;

			DataServiceQuery<WebResult> result = bingSearch.Web(company, "DisableLocationDetection", null, "en-US", null, null, null, null);
			WebResult firstResult = result.FirstOrDefault();

			if (needTwitter)
				return ParseOutTwitterHandle(firstResult);

			if (firstResult != null)
				return firstResult.Url;

			return "";
		}

		static string ParseOutTwitterHandle(WebResult firstResult)
		{
			if (firstResult == null)
				return "";

			string url = firstResult.Url;
			int handleStart = url.IndexOf("://twitter.com/") + "://twitter.com/".Length;
			if (handleStart < "://twitter.com/".Length)
				return url;

			string handle = url.Substring(handleStart);
			if (handle.Contains("/") || handle.Contains("."))
				return handle + "**Problem**";
			else
				return handle;
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

		static void LoadKlout(string connectionString, string kloutAPIKey)
		{
			//CompanyID, KloutID
			Dictionary<int, string> kloutIDs;
			Dictionary<int, string> twitterIDs;
			using (SqlConnection connection = new SqlConnection(connectionString))
			{
				connection.Open();
				//GetCompanies with twitter
				twitterIDs = GetTwitterNames(connection);

				//Load KloutIDs from DB
				kloutIDs = GetKloutIDsStored(connection);
				SqlCommand cmd = new SqlCommand();
				cmd.Connection = connection;

				foreach (KeyValuePair<int, string> twitterID in twitterIDs)
				{
					if (!kloutIDs.ContainsKey(twitterID.Key))
					{
						string kloutID = PullKloutID(twitterID.Value, kloutAPIKey);
						if (kloutID == string.Empty)
							continue;

						kloutIDs.Add(twitterID.Key, kloutID);
						string insertKloutData = $"Insert into KloutScores(CompanyID, KloutID) Values ({twitterID.Key}, {kloutID})";

						Console.Out.WriteLine($"The Klout ID for {twitterID} is {kloutID}");

						cmd.CommandText = insertKloutData;
						cmd.ExecuteNonQuery();
						Thread.Sleep(150);
					}
				}

				string scoreColumn = $"KloutScore{DateTime.Now.ToString("MMMdd")}";

				string addTodaysColumn = $"ALTER TABLE KloutScores ADD {scoreColumn} DECIMAL(16,14)";
				cmd.CommandText = addTodaysColumn;
				cmd.ExecuteNonQuery();

				foreach(KeyValuePair<int, string> kvp in kloutIDs)
				{
					string score = GetKloutScore(kvp.Value, kloutAPIKey);
					string setScore = $"UPDATE KloutScores SET {scoreColumn} = {score} WHERE CompanyID = {kvp.Key}";

					Console.Out.WriteLine($"The Klout score for Company {kvp.Key} is {score}");

					cmd.CommandText = setScore;
					cmd.ExecuteNonQuery();
					Thread.Sleep(150);
				}
			}
		}

		static string GetKloutScore(string kloutID, string kloutAPIKey)
		{
			string kloutScore = kloutBaseAddress + $"user.json/{kloutID}/score?key={kloutAPIKey}";

			WebRequest request = WebRequest.Create(kloutScore);

			WebResponse response = request.GetResponse();
			Stream dataStream = response.GetResponseStream();
			StreamReader sr = new StreamReader(dataStream);

			string responseJson = sr.ReadToEnd();
			var jsonResult = JsonConvert.DeserializeObject(responseJson);
			JObject jsonObject = jsonResult as JObject;
			if (jsonObject == null)
				throw new ArgumentException();

			return jsonObject["score"].Value<string>();
		}

		static string PullKloutID(string twitterID, string apiKey)
		{
			string kloutIdentification = kloutBaseAddress + $"identity.json/twitter?screenName={twitterID}&key={apiKey}";
			WebRequest request = WebRequest.Create(kloutIdentification);

			WebResponse response;
			try
			{
				response = request.GetResponse();
			}
			catch(Exception)
			{
				Console.Out.Write($"Could not find ID for {twitterID}");
				return string.Empty;
			}

			Stream dataStream = response.GetResponseStream();
			StreamReader sr = new StreamReader(dataStream);

			string responseJson = sr.ReadToEnd();
			var jsonResult = JsonConvert.DeserializeObject(responseJson);
			JObject jsonObject = jsonResult as JObject;
			if (jsonObject == null)
				throw new ArgumentException();

			return jsonObject["id"].Value<string>();
		}

		static Dictionary<int, string> GetKloutIDsStored(SqlConnection connection)
		{
			Dictionary<int, string> kloutIDs = new Dictionary<int, string>();

			SqlCommand cmd = new SqlCommand();
			cmd.Connection = connection;

			string getStoredIDs = "SELECT CompanyID, KloutID FROM KloutScores WHERE KloutID <> '' AND KloutID IS NOT NULL";
			cmd.CommandText = getStoredIDs;
			SqlDataReader reader = cmd.ExecuteReader();

			while (reader.Read())
				kloutIDs.Add((int) reader["CompanyID"], (string) reader["KloutID"]);

			reader.Close();

			return kloutIDs;

		}

		static Dictionary<int, string> GetTwitterNames(SqlConnection connection)
		{
			Dictionary<int, string> twitterIDs = new Dictionary<int, string>();

			SqlCommand cmd = new SqlCommand();
			cmd.Connection = connection;

			string getStoredIDs = "SELECT ID, Twitter FROM Company WHERE Twitter <> '' AND Twitter IS NOT NULL";
			cmd.CommandText = getStoredIDs;
			SqlDataReader reader = cmd.ExecuteReader();

			while (reader.Read())
				twitterIDs.Add((int) reader["ID"], (string) reader["Twitter"]);

			reader.Close();
			return twitterIDs;
		}
	}
}
