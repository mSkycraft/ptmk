using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace OpenStreetMapsApp.Data
{
    public class ptmk
    {
        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Launch arguments are missing");
            }
            else
                using (SqlConnection conn = new SqlConnection())
                {
                    string serverName = "localhost";
                    string dbName = "Data";
                    string conLogin = "sa";
                    string conPassword = "1qaz!QAZ";
                    if (File.Exists("config.ini"))
                    {
                        string[] lines = File.ReadAllLines("config.ini");
                        if (lines.Length > 0)
                        {
                            foreach (string line in lines)
                            {
                                string[] buf = line.Split('=');
                                switch (buf[0])
                                {
                                    case "serverName":
                                        serverName = buf[1];
                                        break;
                                    case "dbName":
                                        dbName = buf[1];
                                        break;
                                    case "conLogin":
                                        conLogin = buf[1];
                                        break;
                                    case "conPassword":
                                        conPassword = buf[1];
                                        break;
                                }
                            }
                        }
                    }
                    conn.ConnectionString =
                      $"Data Source={serverName};" +
                      $"Initial Catalog={dbName};" +
                      $"User id={conLogin};" +
                      $"Password={conPassword};" +
                      "TrustServerCertificate=True;";
                    conn.Open();

                    string querry = "";
                    switch (args[0])
                    {
                        case "1": querry = CreateTable(); break;
                        case "2":
                            if (args.Length == 7)
                                querry = InsertToTable(args[1..]);
                            else
                                Console.WriteLine("Not enought args");
                            break;
                        case "3":
                            SqlCommand cmd = conn.CreateCommand();
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = ShowUnic();
                            SqlDataReader reader = cmd.ExecuteReader();
                            if (reader.HasRows)
                            {
                                Console.WriteLine("{0}\t{1}\t{2}\t{3}", reader.GetName(0), reader.GetName(1), reader.GetName(2), reader.GetName(3));
                                while (reader.Read())
                                {
                                    Console.WriteLine($"{reader.GetValue(0)}\t{reader.GetValue(1)}\t{reader.GetValue(2)}\t{reader.GetValue(3)}\t");
                                }
                            }
                            break;
                        case "4":
                            InsertRandom(conn);
                            break;
                        case "5":
                            TimeSpan ts = CheckTime(conn).Elapsed;
                            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                                ts.Hours, ts.Minutes, ts.Seconds,
                                ts.Milliseconds / 10);
                            Console.WriteLine($"Time - {elapsedTime}(hh,mm,sec,ms)");
                            break;
                        case "6":
                            ts = CheckTime(conn).Elapsed;
                            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                                ts.Hours, ts.Minutes, ts.Seconds,
                                ts.Milliseconds / 10);
                            Console.WriteLine($"First time - {elapsedTime}(hh,mm,sec,ms)");
                            cmd = conn.CreateCommand();
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = "CREATE INDEX Full_name_index on Persons([Full name])";
                            cmd.ExecuteNonQuery();
                            TimeSpan ts2 = CheckTime(conn).Elapsed;
                            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                                ts2.Hours, ts2.Minutes, ts2.Seconds,
                                ts2.Milliseconds / 10);
                            Console.WriteLine($"Second time - {elapsedTime}(hh,mm,sec,ms)");
                            TimeSpan diff = ts - ts2;
                            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                                diff.Hours, diff.Minutes, diff.Seconds,
                                diff.Milliseconds / 10);
                            Console.WriteLine($"Difference time - {elapsedTime}(hh,mm,sec,ms)");
                            break;
                    }

                    if (querry != "")
                    {
                        SqlCommand cmd = conn.CreateCommand();
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = querry;
                        cmd.ExecuteNonQuery();
                    }
                }
        }


        public static string CreateTable()
        {
            string createTable = "USE [Data] " +
                "IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Persons]') AND type in (N'U'))" +
                "DROP TABLE [dbo].[Persons] " +
                "CREATE TABLE[dbo].[Persons] (" +
                "[id] [int] IDENTITY(1,1) NOT NULL," +
                "[Full name] [nchar] (50) NOT NULL," +
                "[Birthday] [date] NOT NULL," +
                "[Gender] [nchar](5) NOT NULL," +
                "CONSTRAINT[PK_Persons] PRIMARY KEY CLUSTERED " +
                "([id] ASC)" +
                "WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON[PRIMARY]" +
                ") ON[PRIMARY] ";
            return createTable;
        }

        public static string InsertToTable(string[] args)
        {
            string fullName = args[0] + " " + args[1] + " " + args[2];
            DateTime dateTime = DateTime.Parse(args[3]);

            string insertString = "USE[Data]" +
           "INSERT INTO[dbo].[Persons]" +
               "([Full name]" +
               ",[Birthday]" +
               ",[Gender])" +
           "VALUES" +
               $"('{fullName}'" +
               $",'{dateTime.ToString("yyyy-MM-dd")}'" +
               $",'{args[4]}')";
            return insertString;
        }

        public static string ShowUnic()
        {
            string showString = "select [Full name],Birthday,MAX(Gender) as Gender, DATEDIFF(year,[Birthday],GETDATE())-1 as Diff " +
            "FROM Persons " +
            "GROUP BY [Full name],Birthday " +
            "having count(*)>1 " +
            "Order by [Full name]";

            return showString;
        }

        public static Stopwatch CheckTime(SqlConnection conn)
        {
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = ShowF();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            cmd.ExecuteNonQuery();
            stopwatch.Stop();
            return stopwatch;
        }

        public static string ShowF()
        {
            string showString = "SELECT [id] ,[Full name],[Birthday],[Gender]" +
            "FROM [Data].[dbo].[Persons]"+
            "Where [Full name] LIKE 'F%'";
            return showString;
        }


        public static void InsertRandom(SqlConnection conn)
        {

            for (int i = 0; i < 1000000; i++)
            {
                Console.SetCursorPosition(0, 0);
                Console.WriteLine($"Текущая запись - {i}");
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = InsertToTable(GenFullLine()); ;
                cmd.ExecuteScalarAsync();
                
            }
            for (int i = 0; i < 100; i++)
            {
                Console.SetCursorPosition(0, 0);
                Console.WriteLine($"Текущая запись - {i}");
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = InsertToTable(GenFullLine('F')); ;
                cmd.ExecuteScalarAsync();
            }
        }

        public static string GenString(char start = ' ',bool f = false)
        {
            char[] charsSmall = Enumerable.Range('a', 'z' - 'a' + 1).Select(i => (char)i).ToArray();
            char[] charsBig = Enumerable.Range('A', 'Z' - 'A' + 1).Select(i => (char)i).ToArray();
            string result = "";
            Random random = new Random();
            if(start != ' ')
            {
                result += start;
            }
            else
            {
                result += charsBig[random.Next(0,charsBig.Length-1)];
            }
            int len;
            if (f == false)
                len = random.Next(5, 10);
            else
                len = random.Next(5, 20);
            for (int i = 0; i < len; i++)
            {
                result += charsSmall[random.Next(0, charsSmall.Length - 1)];
            }
            return result;
        }

        public static string[] GenFullLine(char start = ' ')
        {
            List<string> result = new List<string>();
            Random random = new Random();
            result.Add(GenString(start,true));
            result.Add(GenString());
            result.Add(GenString());
            int year = random.Next(1970, 2023);
            int month = random.Next(1, 12);
            int day;
            if ((year%4 ==0)&&(year%100!=0))
                day = random.Next(1, 29);
            else
                day = random.Next(1, 29);
            result.Add($"{year}-{month}-{day}");
            int gender = random.Next(0, 1);
            if (gender == 0)
                result.Add("man");
            else
                result.Add("woman");

            return result.ToArray();
        }
    }
}