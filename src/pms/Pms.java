package pms;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Date;
import java.util.GregorianCalendar;

public class Pms {
	
	Connection dbConn = null;
	String dbUrl = "jdbc:oracle:thin:@localhost:1521:xe";
	String driver = "oracle.jdbc.driver.OracleDriver";
	String dbUsername = "pms";
	String dbPassword = "pms";
	ArrayList<HashMap<String, String>> listOfFunds = new ArrayList<HashMap<String, String>>();
	

	//Display all the available funds in the database
	public void displayAvailableFundsData() {
		
		Statement getFundsStatement = null;
		ResultSet getFundsResultSet = null;
		PreparedStatement getInitialDate = null;
		ResultSet initialDate = null;
		PreparedStatement getLastDate = null;
		ResultSet lastDate = null;
		
		String getFundsQuery = "SELECT DISTINCT (SCHEME_NAME), SCHEME_CODE FROM MASTER";
		String initialDateQuery = "SELECT MIN(DATE_OF_NAV) FROM MASTER WHERE SCHEME_CODE=?"; 
		String lastDateQuery = "SELECT MAX(DATE_OF_NAV) FROM MASTER WHERE SCHEME_CODE=?";
				
		//Making a connection with DB
		try {
			
				Class.forName(driver).newInstance();
				dbConn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
				if (!(dbConn.isClosed()))
					System.out.println("Connection to database is successfull\n");
		
		
				//Fetching all the available funds in the database
				getFundsStatement = dbConn.createStatement();
				getFundsResultSet = getFundsStatement.executeQuery(getFundsQuery);
					
				while(getFundsResultSet.next())
					{
						HashMap<String, String> fund = new HashMap<String, String>();
						String[] sName = getFundsResultSet.getString(1).split("-");
						String formattedSchemeName = sName[0].trim();
						fund.put("schemeName", formattedSchemeName);
						fund.put("schemeCode", getFundsResultSet.getString(2));
						listOfFunds.add(fund);
					}
		
				//Getting the dates for which the data is present for each available fund.
				for(HashMap<String, String> fund : listOfFunds) {
						
						getInitialDate = dbConn.prepareStatement(initialDateQuery);
						getInitialDate.setString(1, fund.get("schemeCode"));
						initialDate = getInitialDate.executeQuery();
						initialDate.next();
						fund.put("dataFrom", initialDate.getString(1));
						
						getLastDate = dbConn.prepareStatement(lastDateQuery);
						getLastDate.setString(1, fund.get("schemeCode"));
						lastDate = getLastDate.executeQuery();
						lastDate.next();
						fund.put("dataTo", lastDate.getString(1));
					
				}				
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
				e.printStackTrace();
			}
			
			//Closing all the statements, results sets and the database connection eventually
			finally {
				try { getFundsStatement.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { getFundsResultSet.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { getInitialDate.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { initialDate.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { getLastDate.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { lastDate.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { dbConn.close(); } catch (Exception e) {e.printStackTrace();}
			}
		
		
		//Displaying the data of the funds available
		System.out.println("Getting the data of the available funds......\n ");
		System.out.println("**********************************************************************************************************");
		System.out.printf("%1$-20s %2$-50s %3$-20s %4$-20s", "Scheme Code", "Scheme Name", "Data From", "Data Upto");
		System.out.println();
		for(HashMap<String, String> maps : listOfFunds) {
			try{				
				String[] formattedFromDate = maps.get("dataFrom").split(" ");
				String[] formattedToDate = maps.get("dataTo").split(" ");
				
				SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat format2 = new SimpleDateFormat("dd MMMM yyyy");
				Date fromDate = format1.parse(formattedFromDate[0]);
				Date toDate = format1.parse(formattedToDate[0]);
				System.out.printf("%1$-20s %2$-50s %3$-20s %4$-20s",maps.get("schemeCode"),maps.get("schemeName"),format2.format(fromDate),format2.format(toDate));
				System.out.println();
			} catch (ParseException e) {
				e.printStackTrace();
			}	
		}
		System.out.println("**********************************************************************************************************");
	}
	
	
	//Method to extract the year, month and day of month in Integer from the date object provided
	public int[] getIntFromDate(Date myDate) {
		Calendar cal = new GregorianCalendar();
		int[] res = new int[3];
		cal.setTime(myDate);
		res[0] = cal.get(Calendar.YEAR);
		res[1] = cal.get(Calendar.MONTH)+1;
		res[2] = cal.get(Calendar.DAY_OF_MONTH);
		return res;
	}
	
	
	// Method to calculate the CAGR of the fund and the whole portfolio
	public void portfolio(ArrayList<HashMap<String, String>> userFunds, Date startDate, Date endDate) {
		
		PreparedStatement getInitialNAV = null;
		ResultSet initialNAVResultSet = null;
		PreparedStatement getFinalNAV = null;
		ResultSet finalNAVResultSet = null;
		String getNAVQuery = "SELECT NAV FROM MASTER WHERE SCHEME_CODE=? AND DATE_OF_NAV BETWEEN ? AND ?";
		SimpleDateFormat dateformat = new SimpleDateFormat("dd-MM-yyyy");
		SimpleDateFormat periodOfConsiderationDateFormat = new SimpleDateFormat("MMMM yyyy");
		Double portfolioIV = 0.0;
		Double portfolioFV = 0.0;
		
		//Get the number of months between the start and the end date provided by the user
		LocalDate sDate = LocalDate.of(getIntFromDate(startDate)[0], getIntFromDate(startDate)[1], getIntFromDate(startDate)[2]);
		LocalDate eDate = LocalDate.of(getIntFromDate(endDate)[0], getIntFromDate(endDate)[1], getIntFromDate(endDate)[2]);
		Period period = Period.between(sDate, eDate);
		int noOfMonths = period.getMonths()+(period.getYears())*12;
			
		for (HashMap<String, String> userMap : userFunds) {			
			try {				
				Class.forName(driver).newInstance();
				dbConn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
				
				//Getting the IV of the fund
				getInitialNAV = dbConn.prepareStatement(getNAVQuery);
				getInitialNAV.setString(1, userMap.get("schemeCode"));
				getInitialNAV.setString(2, dateformat.format(startDate));
				startDate.setDate(10);
				getInitialNAV.setString(3, dateformat.format(startDate));
				startDate.setDate(1);
				initialNAVResultSet = getInitialNAV.executeQuery();
				initialNAVResultSet.next();
				userMap.put("IV", initialNAVResultSet.getString(1));
				
				//Getting the FV of the fund
				getFinalNAV = dbConn.prepareStatement(getNAVQuery);
				getFinalNAV.setString(1, userMap.get("schemeCode"));
				endDate.setDate(1);
				getFinalNAV.setString(2, dateformat.format(endDate));
				endDate.setDate(10);
				getFinalNAV.setString(3, dateformat.format(endDate));
				finalNAVResultSet = getFinalNAV.executeQuery();
				finalNAVResultSet.next();
				userMap.put("FV", finalNAVResultSet.getString(1));
				
				//Calculate the CAGR of the fund
				Double CAGR = calculateCAGR(noOfMonths, Double.parseDouble(userMap.get("IV")), Double.parseDouble(userMap.get("FV")));
				userMap.put("CAGR", CAGR.toString());
				
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			}
			//Closing all the statements, results sets and the database connection eventually
			finally {
				try { getInitialNAV.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { initialNAVResultSet.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { getFinalNAV.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { finalNAVResultSet.close(); } catch (Exception e) { e.printStackTrace(); }
			    try { dbConn.close(); } catch (Exception e) {e.printStackTrace();}
			}
		}
		
		//Calculate the CAGR of the portfolio
		for(HashMap<String, String> userMap : userFunds) {
			portfolioIV = portfolioIV + Double.parseDouble(userMap.get("IV"));
			portfolioFV = portfolioFV + Double.parseDouble(userMap.get("FV"));	
		}
		Double portfolioCAGR = calculateCAGR(noOfMonths, portfolioIV, portfolioFV);
		
		//Displaying the details of the portfolio
		System.out.println();
		System.out.println("Creating the portfolio......\n");
		System.out.println("**********************************************************************************************************");
		System.out.printf("%1$-20s %2$-50s %3$-30s %4$-20s", "Scheme Code", "Scheme Name", "Period of Consideration", "CAGR (%)");
		System.out.println();
		String periodOfConsideration = periodOfConsiderationDateFormat.format(startDate)+" - "+periodOfConsiderationDateFormat.format(endDate); 
		
		for(HashMap<String, String> userMap : userFunds) {
			String CAGRasString = String.format ("%,.2f", Double.parseDouble(userMap.get("CAGR")));
			String displaySchemeName = "";
			for(HashMap<String, String> funds : listOfFunds) {
				if(funds.get("schemeCode").equals(userMap.get("schemeCode"))) {
					displaySchemeName = funds.get("schemeName");
					break;
				}
			}
			System.out.printf("%1$-20s %2$-50s %3$-30s %4$-20s", userMap.get("schemeCode"), displaySchemeName, periodOfConsideration, CAGRasString+" %");
			System.out.println();
		}		
		System.out.println("\nNet CAGR of the whole portfolio = "+String.format("%,.2f", portfolioCAGR)+" % over the period: "+periodOfConsideration);
		System.out.println("**********************************************************************************************************\n");
	}
	
	
	//Method to calculate the CAGR of a fund or portfolio
	public double calculateCAGR(int months, double initialNAV, double finalNAV) {
		
		if(months<=11) {
			Double fundCAGR = ((finalNAV-initialNAV)/initialNAV)*100;
			return fundCAGR;				
		}			
		if(months>=12) {
			Double doubleMonths = (double) months;
			Double t = doubleMonths/12.0;
			Double tRoot = 1.0/t;
			Double temp = Math.pow((finalNAV/initialNAV), tRoot);
			Double fundCAGR = (temp-1)*100;
			return fundCAGR;
		}			
		return 0.0;			
	}
		
	
		public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pms pms = new Pms();
		pms.displayAvailableFundsData();
		System.out.println();
		System.out.println("Do want to create a portfolio? (y/n) : ");
		
		Scanner in = new Scanner(System.in);
		String decision = in.next();
		
		switch (decision) {
		case "y":
			try {
				ArrayList<HashMap<String, String>> fundsDataFromUser = new ArrayList<HashMap<String, String>>();
				
				System.out.println("How many funds will be in your portfolio?");
				Scanner scanFunds = new Scanner(System.in);
				int numberOfFunds = scanFunds.nextInt();
				
				//Getting the Scheme Code of the funds from the user to be added into the portfolio
				System.out.println("Enter the Scheme Code of the available funds that you want to add in your portfolio, seperated by ';' ");
				scanFunds.useDelimiter(";");
				
				for (int i = 1; i<=numberOfFunds; i++) {
						HashMap<String, String> fundData = new HashMap<String, String>();
						fundData.put("schemeCode", scanFunds.next().trim());
						fundsDataFromUser.add(fundData);
					}
				
				//Creating a list of list of available scheme codes in the database
				HashSet<String> availableSchemeCodes = new HashSet<String>();
				for (HashMap<String, String> maps : pms.listOfFunds){
					availableSchemeCodes.add(maps.get("schemeCode"));
				}
				
				//Verify if the user has entered the correct scheme codes
				for(HashMap<String, String> myMap : fundsDataFromUser) {
					if(!availableSchemeCodes.contains(myMap.get("schemeCode"))) {
							System.out.println("The entered Scheme Code: "+myMap.get("schemeCode")+" is not available in the database. Please, re-launch the program");
							System.exit(1);
						}
					}
				
				//Getting the starting and the ending date for the portfolio
				System.out.println("Enter the starting date and ending date seperated by ';'");
				String stDate = "01 "+scanFunds.next().trim();
				String enDate = "10 "+scanFunds.next().trim();
				scanFunds.close();
				SimpleDateFormat format = new SimpleDateFormat("dd MMMM yyyy");
				Date startDateFromUser = format.parse(stDate);
				Date endDateFromUser = format.parse(enDate);
								
				//Verify if the data for the funds is available in the database for the time duration which user has entered.
				for(HashMap<String, String> userMaps : fundsDataFromUser) {					
					for(HashMap<String, String> availableMap : pms.listOfFunds) {
						String sCode = availableMap.get("schemeCode");
						if(userMaps.get("schemeCode").equals(sCode)) {
							String[] formattedFromDate = availableMap.get("dataFrom").split(" ");
							String[] formattedToDate = availableMap.get("dataTo").split(" ");
							SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
							Date fDate = format1.parse(formattedFromDate[0]);
							Date tDate = format1.parse(formattedToDate[0]);
							fDate.setDate(1);
							tDate.setDate(10);
							if(startDateFromUser.before(fDate) || endDateFromUser.after(tDate)) {
								System.out.println("The Scheme Code: "+userMaps.get("schemeCode")+ " does not have the data available in the requested time durations. Please, re-launch the program");
								System.exit(1);
							}					
						}
					}
				}
				
				pms.portfolio(fundsDataFromUser, startDateFromUser, endDateFromUser);
				
				} catch (ParseException e) {
					e.printStackTrace();
				}
			
			break;
			
		case "n":
			break;
			
		default:
			System.out.println("Incorrect option, please re-launch the program ");
			System.exit(1);
			break;
		}
		
		in.close();	
		System.out.println("Good Bye. Happy Investing..!!");
	}
}
