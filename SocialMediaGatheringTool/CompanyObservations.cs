using CenterSpace.NMath.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SocialMediaGatheringTool
{
	public class CompanyObservations
	{
		public enum RegressionType
		{
			Full,
			Delta,
			PercentChanged
		};

		const double AlmostZero = 0.000000001;
		public int CompanyID { get; private set; }
		List<Observation> m_observations;

		public int Length { get { return m_observations.Count; } }

		public CompanyObservations(int companyID)
		{
			CompanyID = companyID;
			m_observations = new List<Observation>();
		}

		/// <summary>
		/// Must be added in order to create proper lags
		/// </summary>
		/// <param name="observe"></param>
		public void AddObservation(Observation observe)
		{
			m_observations.Add(observe);
		}

		public DoubleVector GetPrices(RegressionType type, int size, int lag = 0)
		{
			//Create DataMatrix
			DoubleVector dbleVctr = new DoubleVector(size);
			switch (type)
			{
				case RegressionType.Full:
					if (lag == 0)
					{
						int counter = 0;
						for (int i = Length - size; i < Length; i++)
						{
							dbleVctr[counter] = m_observations[i].StockPrice;
							counter++;
						}
					}
					else
					{
						for (int i = 0; i < size; i++)
							dbleVctr[i] = m_observations[i].StockPrice;
					}
					break;
				case RegressionType.Delta:
					for (int i = 0; i < size; i++)
					{
						double priceDelta = m_observations[i].StockPrice - m_observations[i + lag].StockPrice;
						dbleVctr[i] = priceDelta == 0 ? AlmostZero : priceDelta;
					}
					break;
				case RegressionType.PercentChanged:
					for (int i = 0; i < size; i++)
					{
						double priceDelta = (m_observations[i].StockPrice - m_observations[i + lag].StockPrice)/ m_observations[i].StockPrice;
						dbleVctr[i] = priceDelta == 0 ? AlmostZero : priceDelta;
					}
					break;
			}

			return dbleVctr;
		}

		public DoubleMatrix GetCustomerGenerated(RegressionType type, int lag)
		{
			//Create DataMatrix
			DoubleMatrix dblMtrx = new DoubleMatrix(m_observations.Count - lag, 3);

			switch (type)
			{
				case RegressionType.Full:
					for (int i = 0; i < m_observations.Count - lag; i++)
					{
						dblMtrx[i, 0] = m_observations[i].KloutScore;
						dblMtrx[i, 1] = m_observations[i].NumberFollowers != 0 ? m_observations[i].NumberFollowers : AlmostZero;
						dblMtrx[i, 2] = m_observations[i].StockPrice;
					}
					break;
				case RegressionType.Delta:
					for (int i = 0; i < m_observations.Count - lag; i++)
					{
						dblMtrx[i, 0] = m_observations[i].KloutScore - m_observations[i + lag].KloutScore;
						int folDelta = m_observations[i].NumberFollowers - m_observations[i + lag].NumberFollowers;
						dblMtrx[i, 1] = folDelta != 0 ? folDelta : AlmostZero;
						double stockDelta = m_observations[i].StockPrice - m_observations[i + lag].StockPrice;
						dblMtrx[i, 2] = stockDelta != 0 ? stockDelta : AlmostZero;
					}
					break;
				case RegressionType.PercentChanged:
					for (int i = 0; i < m_observations.Count - lag; i++)
					{
						dblMtrx[i, 0] = (m_observations[i].KloutScore - m_observations[i + lag].KloutScore)/ m_observations[i].KloutScore;
						int folDelta = m_observations[i].NumberFollowers == 0 ? 0 : (m_observations[i].NumberFollowers - m_observations[i + lag].NumberFollowers)/ m_observations[i].NumberFollowers;
						dblMtrx[i, 1] = folDelta != 0 ? folDelta : AlmostZero;
						double stockDelta = (m_observations[i].StockPrice - m_observations[i + lag].StockPrice) / m_observations[i].StockPrice;
						dblMtrx[i, 2] = stockDelta != 0 ? stockDelta : AlmostZero;
					}
					break;
			}
			return dblMtrx;
		}

		public DoubleMatrix GetCompanyGenerated(RegressionType type, int lag)
		{
			//Create DataMatrix
			DoubleMatrix dblMtrx = new DoubleMatrix(m_observations.Count - lag, 5);

			switch (type)
			{
				case RegressionType.Full:
					for (int i = 0; i < m_observations.Count - lag; i++)
					{
						dblMtrx[i, 0] = m_observations[i].KloutScore;
						dblMtrx[i, 1] = m_observations[i].NumberFavorites != 0 ? m_observations[i].NumberFavorites : AlmostZero;
						dblMtrx[i, 2] = m_observations[i].NumberFriends != 0 ? m_observations[i].NumberFriends : AlmostZero;
						dblMtrx[i, 3] = m_observations[i].NumberStatuses != 0 ? m_observations[i].NumberStatuses : AlmostZero;
						dblMtrx[i, 4] = m_observations[i].StockPrice;
					}
					break;
				case RegressionType.Delta:
					for (int i = 0; i < m_observations.Count - lag; i++)
					{
						dblMtrx[i, 0] = m_observations[i].KloutScore - m_observations[i + lag].KloutScore;

						int favDelta = m_observations[i].NumberFavorites - m_observations[i + lag].NumberFavorites;
						dblMtrx[i, 1] = favDelta != 0 ? favDelta : AlmostZero;

						int friDelta = m_observations[i].NumberFriends - m_observations[i + lag].NumberFriends;
						dblMtrx[i, 2] = friDelta != 0 ? friDelta : AlmostZero;

						int statDelta = m_observations[i].NumberStatuses - m_observations[i + lag].NumberStatuses;
						dblMtrx[i, 3] = statDelta != 0 ? statDelta : AlmostZero;

						double stockDelta = m_observations[i].StockPrice - m_observations[i + lag].StockPrice;
						dblMtrx[i, 4] = stockDelta != 0 ? stockDelta : AlmostZero;
					}
					break;
				case RegressionType.PercentChanged:
					for (int i = 0; i < m_observations.Count - lag; i++)
					{
						dblMtrx[i, 0] = (m_observations[i].KloutScore - m_observations[i + lag].KloutScore)/ m_observations[i].KloutScore;

						int favDelta = m_observations[i].NumberFavorites == 0 ? 0 : (m_observations[i].NumberFavorites - m_observations[i + lag].NumberFavorites)/ m_observations[i].NumberFavorites;
						dblMtrx[i, 1] = favDelta != 0 ? favDelta : AlmostZero;

						int friDelta = m_observations[i].NumberFriends == 0 ? 0 : (m_observations[i].NumberFriends - m_observations[i + lag].NumberFriends)/ m_observations[i].NumberFriends;
						dblMtrx[i, 2] = friDelta != 0 ? friDelta : AlmostZero;

						int statDelta = m_observations[i].NumberStatuses == 0 ? 0 : (m_observations[i].NumberStatuses - m_observations[i + lag].NumberStatuses)/ m_observations[i].NumberStatuses;
						dblMtrx[i, 3] = statDelta != 0 ? statDelta : AlmostZero;

						double stockDelta = m_observations[i].StockPrice == 0 ? 0 : (m_observations[i].StockPrice - m_observations[i + lag].StockPrice)/ m_observations[i].StockPrice;
						dblMtrx[i, 4] = stockDelta != 0 ? stockDelta : AlmostZero;
					}
					break;
			}
			return dblMtrx;
		}

	}

	public class Observation
	{
		public double KloutScore { get; private set; }
		public int NumberFavorites { get; private set; }
		public int NumberStatuses { get; private set; }
		public int NumberFollowers { get; private set; }
		public int NumberFriends { get; private set; }
		public double StockPrice { get; private set; }

		public Observation(double kloutScore, int numFav, int numStat, int numFol, int numFriends, double stock)
		{
			KloutScore = kloutScore;
			NumberFavorites = numFav;
			NumberStatuses = numStat;
			NumberFollowers = numFol;
			NumberFriends = numFriends;
			StockPrice = stock;
		}
	}

}
