using Bing;
using CenterSpace.NMath.Core;
using CenterSpace.NMath.Stats;
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
using Twitterizer;

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
				case "LoadTwitterData":
					LoadTwitterData(args[1], args[2], args[3], args[4], args[5]);
					break;
				case "LoadDaily":
					LoadDailyData(args[1], args[2], args[3], args[4], args[5], args[6]);
					break;
				case "CompanyRegression":
					RegressEachCompany(args[1]);
					break;
				default:
					Console.Out.WriteLine("Please enter a proper command");
					return;
			}
		}

		//use centerspace
		static void RegressEachCompany(string connectionString)
		{
			using (SqlConnection connection = new SqlConnection(connectionString))
			{
				connection.Open();
				SqlCommand cmd = new SqlCommand();
				cmd.Connection = connection;

				string getAllData = $"select CompanyID, KloutScore, NumFavorites, NumFriends, NumStatuses, NumFollowers, Price from DataWithLags where CompanyID = 2 order by CompanyID, DateCollected";

				cmd.CommandText = getAllData;
				SqlDataReader reader = cmd.ExecuteReader();

				
				Dictionary<int, CompanyObservations> allObservations = new Dictionary<int, CompanyObservations>();

				while(reader.Read())
				{
					int company = (int) reader["CompanyID"];
					if (!allObservations.ContainsKey(company))
						allObservations.Add(company, new CompanyObservations(company));

					allObservations[company].AddObservation(new Observation(Convert.ToDouble(reader["KloutScore"]), Convert.ToInt32(reader["NumFavorites"]),
						Convert.ToInt32(reader["NumStatuses"]), Convert.ToInt32(reader["NumFollowers"]), Convert.ToInt32(reader["NumFriends"]), Convert.ToDouble(reader["Price"])));
				}
				reader.Close();

				foreach(KeyValuePair<int, CompanyObservations> kvp in allObservations)
					PerformRegressions(kvp.Value, cmd);
			}
		}

		static void PerformRegressions(CompanyObservations compObs, SqlCommand cmd)
		{
			for (int i = 1; i < 6; i++)
				RegressAtLag(compObs, i, cmd);
		}

		static void RegressAtLag(CompanyObservations compObs, int lag, SqlCommand cmd)
		{
			DoubleVector prices = compObs.GetPrices(compObs.Length-lag);
			DoubleVector laggedPrices = compObs.GetPrices(compObs.Length - lag, lag);

			//Regress price & priceLag
			LinearRegression priceRegression = new LinearRegression(new DoubleMatrix(laggedPrices), prices);
			LinearRegressionAnova priceAnova = new LinearRegressionAnova(priceRegression);

			DoubleMatrix companyGen = compObs.GetCompanyGenerated(lag);

			//Regress Price and compLag
			LinearRegression companyRegression = new LinearRegression(companyGen, prices);
			LinearRegressionAnova companyAnova = new LinearRegressionAnova(companyRegression);

			DoubleMatrix customerGen = compObs.GetCustomerGenerated(lag);

			//Regress Price and cust lag
			LinearRegression customerRegression = new LinearRegression(customerGen, prices);
			LinearRegressionAnova customerAnova = new LinearRegressionAnova(customerRegression);

			cmd.CommandText = $"Insert INTO RegressionResults VALUES ({compObs.CompanyID}, {lag}, {priceAnova.AdjustedRsquared}, {customerAnova.AdjustedRsquared}, {companyAnova.AdjustedRsquared})";
			cmd.ExecuteNonQuery();
		}

		static void LoadDailyData(string connectionString, string kloutApiKey, string consumerKey, string consumerSecret, string accessToken, string accessTokenSecret, int startTwitterAt = 0)
		{
			int step = 0;
			try
			{
				LoadKlout(connectionString, kloutApiKey);
				step++;

				LoadTwitterData(connectionString, consumerKey, consumerSecret, accessToken, accessTokenSecret, startTwitterAt);
				step++;

				int countLoaded = 0;
				int countExpected = 890;
				while (countLoaded < countExpected)
				{
					int countAtStart = countLoaded;
					countLoaded += LoadStockData(connectionString);
					if (countLoaded == countAtStart)
						break;
				}
			}
			catch(Exception e)
			{
				StreamWriter sw = new StreamWriter(Directory.GetCurrentDirectory() + DateTime.Now.ToString("yyyyMMdd") + ".log");
				sw.WriteLine($"The Process failed at step {step}");
				sw.WriteLine($"The error is:{e.Message}.");
				sw.WriteLine();
				sw.WriteLine("The stack trace is:");
				sw.Write(e.StackTrace);
				sw.Close();

				throw;
			}

		}

		static void LoadTwitterData(string connectionString, string consumerKey, string consumerSecret, string accessToken, string accessTokenSecret, int startAt = 0)
		{
			OAuthTokens tokens = new OAuthTokens();
			tokens.ConsumerKey = consumerKey;
			tokens.ConsumerSecret = consumerSecret;
			tokens.AccessToken = accessToken;
			tokens.AccessTokenSecret = accessTokenSecret;

			using (SqlConnection connection = new SqlConnection(connectionString))
			{
				connection.Open();
				SqlCommand cmd = new SqlCommand();
				cmd.Connection = connection;

				string getTwitterNames = $"SELECT ID, Twitter FROM Company where Twitter <> '' and Twitter is not null and ID > {startAt}";

				cmd.CommandText = getTwitterNames;
				SqlDataReader reader = cmd.ExecuteReader();
				Dictionary<int, string> twitterIDs = new Dictionary<int, string>();

				while (reader.Read())
					twitterIDs.Add((int) reader["ID"], (string) reader["Twitter"]);

				reader.Close();

				foreach(KeyValuePair<int, string> kvp in twitterIDs)
				{
					TwitterUser user = GetTwitterUser(tokens, kvp.Value);
					if(user == null)
					{
						Thread.Sleep(5000);
						Console.Out.WriteLine($"Skipped {kvp.Value}");
						continue;
					}

					string numFollow = string.Empty;
					if (user.NumberOfFollowers == null)
						numFollow = "NULL";
					else
						numFollow = user.NumberOfFollowers.ToString();

					//(CompanyID int, NumFollowers bigint, NumFavorites bigint, NumFriends bigInt, NumStatuses bigint, DateCollected Date)
					string insertTwitterData = $"INSERT INTO TwitterData VALUES ({kvp.Key}, {numFollow},{user.NumberOfFavorites},{user.NumberOfFriends},{user.NumberOfStatuses}, '{DateTime.Now.ToString("yyyy-MM-dd")}')";
					cmd.CommandText = insertTwitterData;
					cmd.ExecuteNonQuery();

					Console.Out.WriteLine($"Twitter Data added for {kvp.Value}");
					Thread.Sleep(5000);

					//while (LimitExceeded(tokens))
					//	Thread.Sleep(60000);

				}

			}

		}

		static bool LimitExceeded(OAuthTokens tokens)
		{
			TwitterResponse<TwitterRateLimitStatus> response = TwitterRateLimitStatus.GetStatus(tokens);

			if(response.RateLimiting.Remaining == 0)
				Console.Out.WriteLine($"Twitter rate limit hit. Will resume pulling @ {response.RateLimiting.ResetDate.ToString("hh:mm:ss")}");

			return response.RateLimiting.Remaining == 0;
		}

		static int LoadStockData(string connectionString)
		{
			int countLoaded = 0;
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

				foreach (KeyValuePair<int, string> kvp in tickerDictionary)
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
					countLoaded++;
					Console.Out.WriteLine($"Inserted Stock data for {kvp.Value}");
					Thread.Sleep(1500);
				}
			}
			return countLoaded;
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
					if (prof.Type == JTokenType.Integer)
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
			catch (WebException)
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

				foreach (KeyValuePair<int, string> kvp in kloutIDs)
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
			catch (Exception)
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

		static TwitterUser GetTwitterUser(OAuthTokens tokens, string screenName)
		{
			TwitterResponse<TwitterUser> user = TwitterUser.Show(tokens, screenName);
			return user.ResponseObject;
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
