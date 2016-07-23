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

		public DoubleVector GetPrices(int size, int lag = 0)
		{
			//Create DataMatrix
			DoubleVector dbleVctr = new DoubleVector(size);
			if(lag == 0)
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
				for (int i = 0; i < m_observations.Count - lag; i++)
					dbleVctr[i] = m_observations[i].StockPrice;
			}

			return dbleVctr;
		}

		public DoubleMatrix GetCustomerGenerated(int lag)
		{
			//Create DataMatrix
			DoubleMatrix dblMtrx = new DoubleMatrix(m_observations.Count - lag, 3);

			for (int i = 0; i < m_observations.Count - lag; i++)
			{
				dblMtrx[i, 0] = m_observations[i].KloutScore;
				dblMtrx[i, 1] = m_observations[i].NumberFollowers;
				dblMtrx[i, 2] = m_observations[i].StockPrice;
			}

			return dblMtrx;
		}

		public DoubleMatrix GetCompanyGenerated(int lag)
		{
			//Create DataMatrix
			DoubleMatrix dblMtrx = new DoubleMatrix(m_observations.Count - lag, 5);

			for (int i = 0; i < m_observations.Count - lag; i++)
			{
				dblMtrx[i, 0] = m_observations[i].KloutScore;
				dblMtrx[i, 1] = m_observations[i].NumberFavorites;
				dblMtrx[i, 2] = m_observations[i].NumberFriends;
				dblMtrx[i, 3] = m_observations[i].NumberStatuses;
				dblMtrx[i, 4] = m_observations[i].StockPrice;
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
