package stockCrawler;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;


public class StockCrawler {
	public Map<String, Object> initMysqlCon() throws SQLException{
		Map<String, Object> resultMap = new HashMap<String, Object>();
		Connection conn = null;
	    // MySQL��JDBC URL��д��ʽ��jdbc:mysql://�������ƣ����Ӷ˿�/���ݿ������?����=ֵ
	    // ������������Ҫָ��useUnicode��characterEncoding
	    // ִ�����ݿ����֮ǰҪ�����ݿ����ϵͳ�ϴ���һ�����ݿ⣬�����Լ�����
	    // �������֮ǰ��Ҫ�ȴ���javademo���ݿ�
	    String url = "jdbc:mysql://localhost:3306/stock?"
	            + "user=root&password=1234567890&useUnicode=true&characterEncoding=UTF8";
	    try {
	        // ֮����Ҫʹ������������䣬����ΪҪʹ��MySQL����������������Ҫ��������������
	        // ����ͨ��Class.forName�������ؽ�ȥ��Ҳ����ͨ����ʼ������������������������ʽ������
	        Class.forName("com.mysql.jdbc.Driver");// ��̬����mysql����
	        // or:
	        // com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
	        // or��
	        // new com.mysql.jdbc.Driver();
	 
	        System.out.println("�ɹ�����MySQL��������");
	        // һ��Connection����һ�����ݿ�����
	        conn = DriverManager.getConnection(url);
	        // Statement������кܶ෽��������executeUpdate����ʵ�ֲ��룬���º�ɾ����
	        Statement stmt = conn.createStatement();
	        
	        resultMap.put("STATEMENT", stmt);
	        resultMap.put("CONNECT", conn);
	         
	        return resultMap;
	       
	    } catch (SQLException e) {
	        System.out.println("MySQL��������");
	        e.printStackTrace();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
			return null;
		}

	public void endMysqlCon( Map<String, Object> map ) throws SQLException{
		try{
			Statement stmt = (Statement)map.get("STATEMENT");
			Connection conn = (Connection) map.get("CONNECT");
			stmt.close();
            conn.close();
            System.out.println("������������");
	        } catch (SQLException e) {
	            System.out.println("MySQL��������");
	            e.printStackTrace();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	}

	public List<Map<String, Object>> queryMysql( Map<String, Object> map ) throws SQLException{
		try{
			List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
			Map<String, Object> resultMap = new HashMap<String, Object>();
			Statement stmt = (Statement) map.get("STATEMENT");
			ResultSet rS = stmt.executeQuery( map.get("QUERY").toString() );
			ResultSetMetaData rSMD = rS.getMetaData();
			int count = rSMD.getColumnCount();
			int i = 0;
			String[] keySet = new String[count];
			
			for( i = 0; i < count; ++i ){
				keySet[ i ] = rSMD.getColumnName( i + 1 );
			}
			while( rS.next()){
				for( i = 0; i < count; ++i ){
					resultMap.put( keySet[ i ], rS.getString( i + 1 ));
				};
				resultList.add( new HashMap<String, Object>( resultMap ) );
				resultMap.clear();
			}			
			return resultList;
	        } catch (SQLException e) {
	            System.out.println("MySQL��������");
	            e.printStackTrace();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		return null;
	}

	public int updateMysql( Map<String, Object> map ) throws SQLException{
		try{
			Statement stmt = (Statement) map.get("STATEMENT");
			
			return stmt.executeUpdate( map.get("UPDATE").toString() );
	        } catch (SQLException e) {
	            System.out.println(e.getErrorCode() + "\t" + e.getMessage());
	            e.printStackTrace();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		return 0;
	}

	public int setStockInfo( Map<String, Object> map ) throws SQLException, IOException{
			String sqlPrefix = "INSERT INTO STOCK_CAPITAL_HISTORY_DATA( STOCK_CODE, HISTORY_DATE,"
					+ "DAY_CLOSE, DAY_CHANGE, DAY_MAJOR_NET_INFLOW_AMOUNT,"
					+ "DAY_MAJOR_NET_INFLOW_RATIO, DAY_HUGE_NET_INFLOW_AMOUNT, "
					+ "DAY_HUGE_NET_INFLOW_RATIO, DAY_BIG_NET_INFLOW_AMOUNT, "
					+ "DAY_BIG_NET_INFLOW_RATIO, DAY_MIDDLE_NET_INFLOW_AMOUNT, "
					+ "DAY_MIDDLE_NET_INFLOW_RATIO, DAY_SMALL_NET_INFLOW_AMOUNT, "
					+ "DAY_SMALL_NET_INFLOW_RATIO ) VALUES";
			String sqlSuffix = ";";
			
			/**HtmlUnit����webҳ��*/  
	        WebClient webClient = new WebClient();  
	        
	        webClient.getOptions().setJavaScriptEnabled(true); //����JS��������Ĭ��Ϊtrue  
	        webClient.getOptions().setCssEnabled(false); //����css֧�� 
	        webClient.getOptions().setThrowExceptionOnScriptError(false); //js���д���ʱ���Ƿ��׳��쳣     
	        webClient.getOptions().setTimeout(20000);     
	        HtmlPage page = webClient.getPage(map.get("PREFIX").toString() +
					map.get("URLVAR").toString() + map.get("SUFFIX").toString() );  
	      //����Ϊ�������Ҫ
	        String pageXml = page.asXml(); //��xml����ʽ��ȡ��Ӧ�ı�  
	  
	        /**jsoup�����ĵ�*/  
	        Document doc = Jsoup.parse(pageXml);  

			
//			Document doc = Jsoup.connect( map.get("PREFIX").toString() +
//					map.get("URLVAR").toString() + map.get("SUFFIX").toString() )   
//					 .data("query", "Java")   // �������  
//					 .userAgent("I �� m jsoup") // ���� User-Agent   
//					 .cookie("auth", "token") // ���� cookie   
//					 .timeout(30000)           // �������ӳ�ʱʱ��  
//					 .get();                 // ʹ�� POST �������� URL
			Elements trs = doc.select("table").select("tr"); // ������ļ�¼
			System.out.println(trs);
			if( trs.get( 7 ).select("td").size() != 13 ){
				return 0;
			}
			String sqlValue = generateStockInsert( map, trs );
			if( !sqlValue.isEmpty() ){
				int count;
				map.put("UPDATE", sqlPrefix + sqlValue + sqlSuffix);
				System.out.println(sqlPrefix + sqlValue + sqlSuffix);
				count = updateMysql( map );
				if( count < 1 ){
					System.out.println(map.get("UPDATE"));
					throw new SQLException( "����ʧ��" );
				}
				return count;
			}
			return 0;
	}
	
	//ֻ�������һ��/���������
	public String generateStockInsert( Map<String, Object> map, Elements trs ){
		String sql = " ";
		for( int i = 7; i < trs.size(); ++i ){
			Elements tds = trs.get(i).select("td"); // ������¼����
//			if( map.get("DATE").toString().equals( tds.get( 0 ).text() ) ){
			if( "2017-03-17".equals( tds.get( 0 ).text() ) ){
//			if( LocalDate.parse(tds.get( 0 ).text()).isAfter(LocalDate.parse("2017-01-25"))
//					&& LocalDate.parse(tds.get( 0 ).text()).isBefore(LocalDate.now())){
				System.out.println( map.get("DATE") + "\t" + tds.get( 0 ).text() );
				sql += "( '"
						+ map.get("CODE").toString() + "', '"
						+ tds.get( 0 ).text() + "', "
						+ processNum( tds.get( 1 ).text() ) + ", "
						+ processNum( tds.get( 2 ).text() ) + ", "
						+ processNum( tds.get( 3 ).text() ) + ", "
						+ processNum( tds.get( 4 ).text() ) + ", "
						+ processNum( tds.get( 5 ).text() ) + ", "
						+ processNum( tds.get( 6 ).text() ) + ", "
						+ processNum( tds.get( 7 ).text() ) + ", "
						+ processNum( tds.get( 8 ).text() ) + ", "
						+ processNum( tds.get( 9 ).text() ) + ", "
						+ processNum( tds.get( 10 ).text() ) + ", "
						+ processNum( tds.get( 11 ).text() ) + ", "
						+ processNum( tds.get( 12 ).text() ) + "),";				
			}
		}
		return sql.substring(0, sql.length() - 1);
	}
	
	public Object processNum( String num ){
		if( num.substring( num.length() - 1 ).equals( "%" ) ){
			return Float.parseFloat( num.substring( 0, num.length() - 1  )) / 100;
		}else if( num.substring( num.length() - 1 ).equals( "��" )){
			return Float.parseFloat( num.substring( 0, num.length() - 1  )) * 10000;
		}else if( num.substring( num.length() - 1 ).equals( "��" )){
			return Float.parseFloat( num.substring( 0, num.length() - 1  )) * 100000000;
		}else if( num.substring( num.length() - 1 ).equals( "-" )){
			return 0;
		}
		return num;
	}
	
	public void setCodeData( Map<String, Object> map ) throws IOException, SQLException{
		Document doc = Jsoup.connect( map.get("PREFIX").toString() +
				map.get("URLVAR").toString() + map.get("SUFFIX").toString() )   
				 .data("query", "Java")   // �������  
				 .userAgent("I �� m jsoup") // ���� User-Agent   
				 .cookie("auth", "token") // ���� cookie   
				 .timeout(10000)           // �������ӳ�ʱʱ��  
				 .get();   // ʹ�� GET �������� URL
		String[] tmp = null;
		String sqlPrefix = "INSERT INTO STOCK_CODE_REPOSITORY VALUES";
		String sqlSuffix = ";";
		String sqlVar = "";
		
		//ƥ����������ȡ��ҳ��
		String regex = "(?<=pages:).*?(?=,)";
		Pattern pattern = Pattern.compile( regex );
		Matcher matcher = pattern.matcher( doc.toString() );
		matcher.find();
		int pages = Integer.parseInt( matcher.group() );

		//ÿҳ��data�ű�����
		regex = "(?<=data:\\[).*?(?=\\]\\})";
		pattern = Pattern.compile( regex );
		for( int i = 1; i <= pages; ++i ){
			doc = Jsoup.connect( map.get("PREFIX").toString() +
					i + map.get("SUFFIX").toString() )   
					 .data("query", "Java")   // �������  
					 .userAgent("I �� m jsoup") // ���� User-Agent   
					 .cookie("auth", "token") // ���� cookie   
					 .timeout(10000)           // �������ӳ�ʱʱ��  
					 .get();   // ʹ�� GET �������� URL
			matcher = pattern.matcher( doc.toString() );
			matcher.find();
			tmp = matcher.group().split("\",\"");
			//������¼����ȡ��Ʊ���벢ƴ��
			for( int j = 0; j < tmp.length; ++j){
				sqlVar += "('" + tmp[ j ].split(",")[1] + "'),";
			}
			System.out.println( i );
		}
		map.put("UPDATE", sqlPrefix + sqlVar.substring(0, sqlVar.length() - 1 ) + sqlSuffix);
		System.out.println( updateMysql( map ) );
	}
	
	public List<Map<String, Object>> getCodeData( Map<String, Object> map ) throws IOException, SQLException{
		
		map.put("QUERY", "SELECT"
				+ " * "
				+ "FROM "
				+ "STOCK_CODE_REPOSITORY "
				+ "WHERE "
				+ "STOCK_CODE LIKE '0%' "
				+ "OR STOCK_CODE LIKE '6%';");
		List<Map<String, Object>> codeList = queryMysql(map);
		return codeList;
	}
	
	public void setMaxMinCloseInfo( Map<String, Object> map ) throws SQLException, ParseException{
		List<Map<String, Object>> codeList = null;
		List<Map<String, Object>> dataList = null;
		int count = 0;
		double dayClose = 0;
		double maxDayClose = 0;
		String maxCloseDate = "";
		double minDayClose = Double.MAX_VALUE;
		String minCloseDate = "";
		String closeDate = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		StringBuffer commitSql = new StringBuffer( "REPLACE INTO "
				+ "STOCK_CAPITAL_HISTORY_DATA_MAX_MIN( "
				+ "STOCK_CODE, "
				+ "MAX_HISTORY_DATE, "
				+ "MAX_DAY_CLOSE, "
				+ "MIN_HISTORY_DATE, "
				+ "MIN_DAY_CLOSE ) "
				+ "VALUES" );
		
		//��ȡȫ��code�����ں����̼ۣ���code�����̼�����ÿ��code�ĵ�һ�����̼ۼ���ͼ�
		map.put("QUERY", "SELECT "
				+ "STOCK_CODE, "
				+ "HISTORY_DATE, "
				+ "DAY_CLOSE "
				+ "FROM "
				+ "STOCK_CAPITAL_HISTORY_DATA "
				+ "WHERE "
				+ "HISTORY_DATE BETWEEN '2016-07-01' AND '2017-01-24' "
				+ "ORDER BY "
				+ "STOCK_CODE, "
				+ "DAY_CLOSE ASC");
		System.out.println( map.get( "QUERY" ) );
		dataList = queryMysql( map );
		//�����ݽṹ��code���оۺ�
		dataList = collectData( "STOCK_CODE", dataList );
		//��ÿ��code�����ݣ��ҳ���ֵ����Ӧ����
		//������Сֵ��������Сֵ����֮������ֵ
		for( Map<String, Object> dataMap : dataList ){
			minDayClose = Double.parseDouble( ( ( List<Map<String, Object>> )dataMap.get( "DATA_LIST" ) )
					.get( 0 ).get( "DAY_CLOSE" ).toString() );
			minCloseDate = ( ( List<Map<String, Object>> )dataMap.get( "DATA_LIST" ) )
					.get( 0 ).get( "HISTORY_DATE" ).toString();
			for( Map<String, Object> eachDataMap : ( List<Map<String, Object>> )dataMap.get( "DATA_LIST" ) ){
				dayClose = Double.parseDouble( eachDataMap.get("DAY_CLOSE").toString() );
				closeDate = eachDataMap.get("HISTORY_DATE").toString();
				//���ǲ������Σ���������ʱ���ֵ5%���򲻸�����ʱ���ֵ
				if( dayClose > ( maxDayClose * 1.05 ) && sdf.parse( closeDate ).getTime() 
						>= sdf.parse( minCloseDate ).getTime() ){
					maxDayClose = dayClose;
					maxCloseDate = closeDate;
				}
			}
			//��ÿ��code��Ӧ����ֵ���ݴ������
			commitSql.append( "("
					+ "\'" + dataMap.get( "STOCK_CODE" ) + "\', "
					+ "\'" + maxCloseDate + "\', "
					+ "" + maxDayClose + ", "
					+ "\'" + minCloseDate + "\', "
					+ "" + minDayClose + "), " );

			//��ֵ��ʼ��
			maxDayClose = 0;
			minDayClose = Double.MAX_VALUE;
			//��ӡ�������code
			System.out.println( dataMap.get( "STOCK_CODE" ) );
		}
			
		commitSql.replace( commitSql.lastIndexOf( "," ), commitSql.lastIndexOf( "," ) + 1, ";");
		System.out.println( commitSql );
		map.put("UPDATE", commitSql);
		//��ӡ������
		System.out.println( updateMysql( map ) );
	}
	
	public void setStockDailyData( Map<String, Object> map ) throws IOException, SQLException, InterruptedException{
		int failCount = 0;
		int updateCount = 0;
		//��ȡ���ݿ���������0��6��ͷ�Ĺ�Ʊ����
		List<Map<String, Object>> codeList = getCodeData( map );
		//����ȡ��Ʊ�����Ӧ����ʷ���ݣ��Դ���Ϊ��λ�������ݿ�
//		map.put("PREFIX", "http://data.eastmoney.com/zjlx/");
//		map.put("SUFFIX", ".html");
		//���ӿڻ�ȡ
		map.put("PREFIX", "http://ff.eastmoney.com//EM_CapitalFlowInterface/api/js?type=hff&rtntype=2"
				+ "&js=({data:(x)})&cb=var%20aff_data=&check=TMLBMSPROCR&id=");
		Date date = new Date();
		SimpleDateFormat sDF = new SimpleDateFormat( "yyyy-MM-dd" );
		for( int i = 0; i < codeList.size(); ++i ){
			map.put( "URLVAR", codeList.get( i ).get( "STOCK_CODE" ) );
			map.put( "CODE", codeList.get( i ).get( "STOCK_CODE" ) );
			if(map.get("CODE").toString().startsWith("0")){
				map.put("SUFFIX", "2&_=1489923789807");
			}else if(map.get("CODE").toString().startsWith("6")){
				map.put("SUFFIX", "1&_=1489923789807");				
			}
			map.put( "DATE", sDF.format(date) );
			System.out.println( "��ȡ���룺\t" + codeList.get( i ).get( "STOCK_CODE" ) );
			System.out.println( "��ȡ��n����������\t" + i );
			try{
				updateCount += setStockInfo2( map );
			}catch( IOException e ){
				System.out.println( e.getMessage() );
				++failCount;
				continue;
			}
			Thread.sleep(200);
		}
		System.out.println( "����ʧ�ܴ�����\t" + failCount );
		System.out.println( "�ܸ���������\t" + updateCount );
	}
	
	//Ĭ���б����һ��Ԫ��Ϊ�������ݣ�ʹ���б�����Ԫ�ؼ���kdjֵ
	public Map<String, Object> calculateKDJ( List<Map<String, Object>> list, double kLast, double dLast ){
		Map<String, Object> retMap = new HashMap<>();
		double cur = Double.parseDouble( list.get( list.size() - 1 ).get( "DAY_CLOSE" ).toString() );
		double lowest = lowestPrice( list );
		double highest = highestPrice( list );
		double rsv = highest == lowest ? 0 : ( cur - lowest ) / ( highest - lowest ) * 100;
		double k = ( rsv + 2 * kLast ) / 3;
		double d = ( k + 2 * dLast ) / 3;
		retMap.put( "K", k );
		retMap.put( "D", d );
		retMap.put( "J", 3 * k - 2 * d );
		retMap.put( "HISTORY_DATE", list.get( list.size() - 1 ).get( "HISTORY_DATE" ).toString() );
		return retMap;
	}
	
	public double lowestPrice( List<Map<String, Object>> list ){
		double lowest = Double.MAX_VALUE;
		for( Map<String, Object> map : list ){
			if( Double.parseDouble( map.get( "DAY_CLOSE" ).toString() ) < lowest ){
				lowest = Double.parseDouble( map.get( "DAY_CLOSE" ).toString() );
			}
		}
		return lowest;
	}
	
	public double highestPrice( List<Map<String, Object>> list ){
		double highest = Double.MIN_VALUE;
		for( Map<String, Object> map : list ){
			if( Double.parseDouble( map.get( "DAY_CLOSE" ).toString() ) > highest ){
				highest = Double.parseDouble( map.get( "DAY_CLOSE" ).toString() );
			}
		}
		return highest;
	}
	
	public List<Map<String,Object>> getKDJList( List<Map<String, Object>> list, int n ){
		double k = 50;
		double d = 50;
		List<Map<String,Object>> kDJList = new ArrayList<>();
		
		for( int i = 1; i <= list.size(); ++i ){
			Map<String,Object> kDJMap = new HashMap<>();
			kDJMap = calculateKDJ( list.subList( i > n ? i - n : 0, i ), k, d );
			k = Double.parseDouble( kDJMap.get( "K" ).toString() );
			d = Double.parseDouble( kDJMap.get( "D" ).toString() );
			kDJList.add( kDJMap );
			}
		
		return kDJList;
	}
	
	public void setKDJ( Map<String, Object> map ) throws SQLException, IOException{
		List<Map<String, Object>> codeList = getCodeData( map );
		int count = 0;
		int n = 9;
		for( Map<String, Object> codeMap : codeList ){
			map.put("QUERY", "SELECT "
					+ "STOCK_CODE, "
					+ "HISTORY_DATE, "
					+ "DAY_CLOSE "
					+ "FROM "
					+ "STOCK_CAPITAL_HISTORY_DATA "
					+ "WHERE "
					+ "STOCK_CODE = \'" + codeMap.get( "STOCK_CODE" ).toString() + "\'"
//					+ "STOCK_CODE = \'" + "000002" + "\'"
					+ "ORDER BY "
					+ "HISTORY_DATE ASC");
			List<Map<String, Object>> dayInfoList = queryMysql( map );
			if( !dayInfoList.isEmpty() ){
				map.put( "KDJ_LIST", getKDJList( dayInfoList, n ) );
				map.put( "CODE", codeMap.get( "STOCK_CODE" ) );
				count += insertKDJTable( map );
			}
		}
		System.out.println( count );
	}
	
	//����ָ�꣬�趨�����ǩ
	int setClassificationLabel( Map<String, Object> map ) throws SQLException{
		List<Map<String, Object>> labelList = calcLabel( map );
		return insertLabel( map, labelList );
	}
	
	List<Map<String, Object>> calcLabel( Map<String, Object> map ) throws SQLException{
		map.put( "QUERY", "SELECT "
				+ "STOCK_CODE, "
				+ "IF((( MAX_DAY_CLOSE / MIN_DAY_CLOSE ) - 1 > 0.04 "
				+ "AND DATEDIFF( MAX_HISTORY_DATE, MIN_HISTORY_DATE ) < 120 ),1 , 0) AS LABEL "
				+ "FROM "
				+ "STOCK_CAPITAL_HISTORY_DATA_MAX_MIN "
				+ ";" );
		return queryMysql( map );
	}
	
	int insertLabel( Map<String, Object> map, List<Map<String, Object>> labelList ) throws SQLException{
		StringBuffer updateSql = new StringBuffer("REPLACE INTO "
				+ "CLASSIFICATION_LABEL "
				+ "VALUES");
		for( Map<String, Object> labelMap : labelList ){
			updateSql.append( "(" + labelMap.get( "STOCK_CODE" ) + "," + labelMap.get( "LABEL" ) + ")," );
		}
		updateSql.replace( updateSql.length() - 1, updateSql.length(), ";" );
		map.put( "UPDATE", updateSql.toString() );
		return updateMysql( map );
	}
	
	//�������ݿ��л�ȡ���м�¼�ۼ��ɲ�θ��ḻ�Ľṹ
	List<Map<String, Object>> collectData( String collectionTargetName, List<Map<String, Object>> dataList ){
		System.out.println( dataList.size() );
		
		String curTarget = dataList.get( 0 ).get( collectionTargetName ).toString();
		List<Map<String, Object>> tmpList = new ArrayList<>();
		Map<String, Object> tmpMap = new HashMap<>();
		List<Map<String, Object>> retList = new ArrayList<>();
		
		for( Map<String, Object> dataMap : dataList ){
			if( curTarget.equals( dataMap.get( collectionTargetName ) ) ){
				tmpList.add( dataMap );
			}else{
				//������һ��code�ļ�¼
				tmpMap.put( collectionTargetName, curTarget );
				tmpMap.put( "DATA_LIST", new ArrayList<>( tmpList ) );
				retList.add( new HashMap<>( tmpMap ) );
				curTarget = dataMap.get( collectionTargetName ).toString();
				//�����ʱ����
				tmpList.clear();
				tmpMap.clear();
				//��������������code��¼
				tmpList.add( dataMap );
			}
		}
		
		System.out.println( retList.size() );
		
		return retList;
	}
	
	int insertKDJTable( Map<String, Object> map ) throws SQLException{
		StringBuffer sql = new StringBuffer( "REPLACE INTO KDJ_DATA VALUES" );
		for( Map<String, Object> kDJMap : (List<Map<String, Object>>) map.get( "KDJ_LIST" ) ){
			sql.append( "(\'" + map.get( "CODE" ) + "\',\'" + kDJMap.get( "HISTORY_DATE" ) + "\',"
					+ kDJMap.get( "K" ) + "," + kDJMap.get( "D" ) + "," + kDJMap.get( "J" ) + ")," );
		}
		sql.replace( sql.length() - 1, sql.length(), ";");
		map.put( "UPDATE", sql.toString() );
		System.out.println( map.get( "CODE" ).toString() );
		return updateMysql( map );
	}
	
	public void analyzeQulification( Map<String, Object> map ) throws SQLException, IOException{
		List<Map<String, Object>> codeList = getCodeData( map );
		int conCount = 0;
		int resCount = 0;
		double gap = 0;
		double sellPrice = 0;
		double buyPrice = 0;
		double expectedRate = 0;
		double expectGap = 0;
		double totalRate = 0;
		double totalGap = 0;
		Map<String, Object> intersetMap = new HashMap<>();
		
		for( Map<String, Object> codeMap : codeList ){
			map.put("QUERY", "SELECT "
					+ "SCHD.STOCK_CODE, "
					+ "SCHD.HISTORY_DATE, "
					+ "SCHD.DAY_CLOSE,"
					+ "SCHD.DAY_MAJOR_NET_INFLOW_AMOUNT, "
					+ "KD.K, "
					+ "KD.D, "
					+ "KD.J, "
					+ "MD.EMAF, "
					+ "MD.EMAS, "
					+ "MD.DIFF, "
					+ "MD.DMA "
					+ "FROM "
					+ "STOCK_CAPITAL_HISTORY_DATA SCHD "
					+ "INNER JOIN KDJ_DATA KD "
					+ "ON SCHD.STOCK_CODE = KD.STOCK_CODE "
					+ "AND SCHD.HISTORY_DATE = KD.HISTORY_DATE "
					+ "INNER JOIN MACD_DATA MD "
					+ "ON SCHD.STOCK_CODE = MD.STOCK_CODE "
					+ "AND SCHD.HISTORY_DATE = MD.HISTORY_DATE "
					+ "INNER JOIN POTENTIAL_CHANCE PC "
					+ "ON SCHD.STOCK_CODE = PC.STOCK_CODE "
					+ "WHERE "
					+ "SCHD.STOCK_CODE = \'" + codeMap.get( "STOCK_CODE" ).toString() + "\'"
//					+ "SCHD.STOCK_CODE = \'" + "000001" + "\' "
					+ "AND PC.CHANCE_RATE > 0.5 "
					+ "AND PC.STOCK_RECORD_LENGTH > 70 "
					+ "ORDER BY "
					+ "SCHD.HISTORY_DATE ASC");
			List<Map<String, Object>> dayInfoList = queryMysql( map );
			if(!dayInfoList.isEmpty()){
				for(int i = 1; i < dayInfoList.size(); ++i){
					//����ָ��
					//����
					if(buyingSignal(dayInfoList, i)){
						++conCount;
						//����Ǳ������
//						intersetMap = calcPotentialInterest(dayInfoList, i);
						//����ʵ������
						intersetMap = calcSellInterest(dayInfoList, i);
						buyPrice = Double.parseDouble(intersetMap.get("BUY_PRICE").toString());
						sellPrice = Double.parseDouble(intersetMap.get("SELL_PRICE").toString());
						gap = Double.parseDouble(intersetMap.get("GAP").toString());
						//������СֵΪ0�������Сֵ�������������0
						if(0 == buyPrice || gap <= 0){
							break;
						}
//						System.out.println(highest / lowest - 1);
						if(sellPrice / buyPrice > 1.05){
							++resCount;
						}
						totalRate += sellPrice / buyPrice - 1;
						totalGap += gap;
					}
				}
//				System.out.println(dayInfoList.size());
				System.out.println(codeMap.get( "STOCK_CODE" ).toString());
			}
		}
		System.out.println( conCount );
		System.out.println( resCount );
		System.out.println( resCount * 1.0 / conCount );
		System.out.println( totalRate / conCount );
		System.out.println( totalGap / conCount );
	}
	
	public Map<String, Object> lowestPriceDay( List<Map<String, Object>> list ){
		double lowest = Double.MAX_VALUE;
		Map<String, Object> retDay = new HashMap<>();
		for( Map<String, Object> map : list ){
			if( Double.parseDouble( map.get( "DAY_CLOSE" ).toString() ) < lowest ){
				lowest = Double.parseDouble( map.get( "DAY_CLOSE" ).toString() );
				retDay = map;
			}
		}
		return retDay;
	}
	
	public Map<String, Object> highestPriceDay( List<Map<String, Object>> list ){
		double highest = Double.MIN_VALUE;
		Map<String, Object> retDay = new HashMap<>();
		for( Map<String, Object> map : list ){
			if( Double.parseDouble( map.get( "DAY_CLOSE" ).toString() ) > highest ){
				highest = Double.parseDouble( map.get( "DAY_CLOSE" ).toString() );
				retDay = map;
			}
		}
		return retDay;
	}
	
	public boolean buyingSignal(List<Map<String, Object>> dayInfoList, int i){
//		return Double.parseDouble(dayInfoList.get(i).get("K").toString())
//				> Double.parseDouble(dayInfoList.get(i).get("D").toString())
//			&& Double.parseDouble(dayInfoList.get(i-1).get("K").toString())
//				<= Double.parseDouble(dayInfoList.get(i-1).get("D").toString())
//			&& Double.parseDouble(dayInfoList.get(i).get("D").toString()) <= 30;
		return Double.parseDouble(dayInfoList.get(i - 1).get("J").toString()) < 0 &&
				Double.parseDouble(dayInfoList.get(i - 1).get("J").toString()) 
				< Double.parseDouble(dayInfoList.get(i).get("J").toString()) &&
				Double.parseDouble(dayInfoList.get(i - 1).get("DMA").toString()) < 0 &&
				Double.parseDouble(dayInfoList.get(i - 1).get("DMA").toString()) 
				< Double.parseDouble(dayInfoList.get(i).get("DMA").toString()) &&
				Double.parseDouble(dayInfoList.get(i).get("DAY_MAJOR_NET_INFLOW_AMOUNT").toString()) > 0;				
	}
	
	public boolean sellingSignal(List<Map<String, Object>> dayInfoList, int j){
//		return Double.parseDouble(dayInfoList.get(j).get("K").toString())
//				<= Double.parseDouble(dayInfoList.get(j).get("D").toString())
//				&& Double.parseDouble(dayInfoList.get(j-1).get("K").toString())
//					> Double.parseDouble(dayInfoList.get(j-1).get("D").toString())
//				&& Double.parseDouble(dayInfoList.get(j).get("D").toString()) >= 60;
		return Double.parseDouble(dayInfoList.get(j - 1).get("J").toString()) 
				>= Double.parseDouble(dayInfoList.get(j).get("J").toString())
				|| Double.parseDouble(dayInfoList.get(j).get("DMA").toString()) > 0 
				|| Double.parseDouble(dayInfoList.get(j - 1).get("DMA").toString())
				>= Double.parseDouble(dayInfoList.get(j).get("DMA").toString()) ;
//				|| Double.parseDouble(dayInfoList.get(j).get("DAY_MAJOR_NET_INFLOW_AMOUNT").toString()) < 0;

	}
	
	public Map<String, Object> calcPotentialInterest(List<Map<String, Object>> dayInfoList, int i){
		Map<String, Object> retMap = new HashMap<>();
		double gap = 0;
		double highest = 0;
		double lowest = 0;
		int dayInterval = 45;
		Map<String, Object> highestDay = new HashMap<>();
		Map<String, Object> lowestDay = new HashMap<>();
		highestDay = highestPriceDay(dayInfoList.subList(i, dayInfoList.size() > i + dayInterval 
				? i + dayInterval : dayInfoList.size()));
		lowestDay = lowestPriceDay(dayInfoList.subList(i > 4 ? i - 5 : 0, dayInfoList.size() > i + 5 
				? i + 5 : dayInfoList.size()));
		highest = Double.parseDouble(highestDay.get("DAY_CLOSE").toString());
		lowest = Double.parseDouble(lowestDay.get("DAY_CLOSE").toString());
		gap = LocalDate.parse(highestDay.get("HISTORY_DATE").toString()).toEpochDay() - 
				LocalDate.parse(lowestDay.get("HISTORY_DATE").toString()).toEpochDay();
		
		retMap.put("SELL_PRICE", highest);
		retMap.put("BUY_PRICE", lowest);
		retMap.put("GAP", gap);
		return retMap;

	}
	
	public Map<String, Object> calcSellInterest(List<Map<String, Object>> dayInfoList, int i){
		Map<String, Object> retMap = new HashMap<>();
		double gap = 0;
		double sellPrice = 0;
		double buyPrice = 0;
		if(i < dayInfoList.size() - 1){
			//�޺�������ʱ���Ļ�����
			buyPrice = Double.parseDouble(dayInfoList.get(i).get("DAY_CLOSE").toString());
			sellPrice = Double.parseDouble(dayInfoList.get(i).get("DAY_CLOSE").toString());
			gap = LocalDate.parse(dayInfoList.get(i).get("HISTORY_DATE").toString()).toEpochDay() - 
					LocalDate.parse(dayInfoList.get(i).get("HISTORY_DATE").toString()).toEpochDay();
			for(int j = i + 1; j < dayInfoList.size(); ++j){
				if(sellingSignal(dayInfoList, j)){
					sellPrice = Double.parseDouble(dayInfoList.get(j).get("DAY_CLOSE").toString());
					gap = LocalDate.parse(dayInfoList.get(j).get("HISTORY_DATE").toString()).toEpochDay() - 
							LocalDate.parse(dayInfoList.get(i).get("HISTORY_DATE").toString()).toEpochDay();
					break;
				}
			}
		}
		retMap.put("SELL_PRICE", sellPrice);
		retMap.put("BUY_PRICE", buyPrice);
		retMap.put("GAP", gap);
		return retMap;

	}

	public void setMACD( Map<String, Object> map ) throws SQLException, IOException{
		List<Map<String, Object>> codeList = getCodeData( map );
		int count = 0;
		for( Map<String, Object> codeMap : codeList ){
			map.put("QUERY", "SELECT "
					+ "STOCK_CODE, "
					+ "HISTORY_DATE, "
					+ "DAY_CLOSE "
					+ "FROM "
					+ "STOCK_CAPITAL_HISTORY_DATA "
					+ "WHERE "
					+ "STOCK_CODE = \'" + codeMap.get( "STOCK_CODE" ).toString() + "\'"
//					+ "STOCK_CODE = \'" + "000002" + "\'"
					+ "ORDER BY "
					+ "HISTORY_DATE ASC");
			List<Map<String, Object>> dayInfoList = queryMysql( map );
			if(!dayInfoList.isEmpty()){
				map.put("MACD_LIST", getMACDList(dayInfoList, 12, 26, 9));
				map.put("CODE", codeMap.get( "STOCK_CODE"));
//				map.put("CODE", "000002");
				count += insertMACDTable( map );
			}
		}
		System.out.println( count );
	}
	
	public List<Map<String,Object>> getMACDList( List<Map<String, Object>> list, int eMAFD, int eMASD, int dEAD ){
		double lastEMAF = 0;
		double lastEMAS = 0;
		double lastDMA = 0;
		double eMAF = 0;
		double eMAS = 0;
		double diff = 0;
		List<Map<String,Object>> mACDList = new ArrayList<>();
		
		if( list != null && !list.isEmpty() ){
			for( int i = 0; i < list.size(); ++i ){
				Map<String,Object> mACDMap = new HashMap<>();
				if(i > 0){
					lastEMAF = Double.parseDouble(mACDList.get(mACDList.size() - 1).get("EMAF").toString());
					lastEMAS = Double.parseDouble(mACDList.get(mACDList.size() - 1).get("EMAS").toString());				
				}else{
					lastEMAF = 0;
					lastEMAS = 0;
				}
				eMAF = lastEMAF * (eMAFD - 1) / (eMAFD + 1) +
						Double.parseDouble(list.get(i).get("DAY_CLOSE").toString()) * 2 / (eMAFD + 1);
				eMAS = lastEMAS * (eMASD - 1) / (eMASD + 1) +
						Double.parseDouble(list.get(i).get("DAY_CLOSE").toString()) * 2 / (eMASD + 1);
				mACDMap.put("EMAF", eMAF);
				mACDMap.put("EMAS", eMAS);
				mACDMap.put("DIFF", eMAF - eMAS);
				mACDMap.put("HISTORY_DATE", list.get(i).get("HISTORY_DATE"));
				mACDList.add( mACDMap );
			}
			for(int i = 0; i < mACDList.size(); ++i){
				if(i > 0){
					lastDMA = Double.parseDouble(mACDList.get(i - 1).get("DMA").toString());
				}else{
					lastDMA = 0;
				}
				mACDList.get(i).put("DMA", lastDMA * (dEAD - 1) / (dEAD + 1) +
						Double.parseDouble(mACDList.get(i).get("DIFF").toString()) * 2 / (dEAD + 1));
			}
		}
		return mACDList;
	}
	
	int insertMACDTable( Map<String, Object> map ) throws SQLException{
		StringBuffer sql = new StringBuffer( "REPLACE INTO MACD_DATA VALUES" );
		for( Map<String, Object> mACDMap : (List<Map<String, Object>>) map.get( "MACD_LIST" ) ){
			sql.append( "(\'" + map.get( "CODE" ) + "\',\'" + mACDMap.get( "HISTORY_DATE" ) + "\',"
					+ mACDMap.get( "EMAF" ) + "," + mACDMap.get( "EMAS" ) + "," + mACDMap.get( "DIFF" ) + "," + mACDMap.get( "DMA" ) + ")," );
		}
		sql.replace( sql.length() - 1, sql.length(), ";");
		map.put( "UPDATE", sql.toString() );
		System.out.println( map.get( "CODE" ).toString() );
//		System.out.println( sql );
		return updateMysql( map );
	}
	
	public void potentialChance( Map<String, Object> map ) throws SQLException, IOException{
		List<Map<String, Object>> codeList = getCodeData( map );
		int totalChance = 0;
		int expectedChance = 0;
		int totalExpectedChance = 0;
		StringBuffer sql = new StringBuffer( "REPLACE INTO POTENTIAL_CHANCE VALUES" );
		
		for( Map<String, Object> codeMap : codeList ){
			map.put("QUERY", "SELECT "
					+ "SCHD.STOCK_CODE, "
					+ "SCHD.HISTORY_DATE, "
					+ "SCHD.DAY_CLOSE,"
					+ "SCHD.DAY_MAJOR_NET_INFLOW_AMOUNT, "
					+ "KD.K, "
					+ "KD.D, "
					+ "KD.J, "
					+ "MD.EMAF, "
					+ "MD.EMAS, "
					+ "MD.DIFF, "
					+ "MD.DMA "
					+ "FROM "
					+ "STOCK_CAPITAL_HISTORY_DATA SCHD "
					+ "INNER JOIN KDJ_DATA KD "
					+ "ON SCHD.STOCK_CODE = KD.STOCK_CODE "
					+ "AND SCHD.HISTORY_DATE = KD.HISTORY_DATE "
					+ "INNER JOIN MACD_DATA MD "
					+ "ON SCHD.STOCK_CODE = MD.STOCK_CODE "
					+ "AND SCHD.HISTORY_DATE = MD.HISTORY_DATE "
					+ "WHERE "
					+ "SCHD.STOCK_CODE = \'" + codeMap.get( "STOCK_CODE" ).toString() + "\'"
//					+ "SCHD.STOCK_CODE = \'" + "000001" + "\' "
					+ "ORDER BY "
					+ "SCHD.HISTORY_DATE ASC");
			List<Map<String, Object>> dayInfoList = queryMysql( map );
			if(!dayInfoList.isEmpty()){
				int size = dayInfoList.size();
				totalChance += size * (size - 1) / 2;
				for(int i = 0; i < size - 1; ++i){
					double buyPrice = Double.parseDouble(dayInfoList.get(i).get("DAY_CLOSE").toString());
					for(int j = i + 1; j < size; ++j){
						double sellPrice = Double.parseDouble(dayInfoList.get(j).get("DAY_CLOSE").toString());
						if(sellPrice / buyPrice > 1.05){
							++expectedChance;
						}
					}
				}
//				System.out.println(dayInfoList.size());
				sql.append("('" + codeMap.get( "STOCK_CODE" ) + "'," + size + "," + expectedChance * 1.0 / (size * (size - 1) / 2) + "),");
				System.out.println(codeMap.get( "STOCK_CODE" ).toString());
				totalExpectedChance += expectedChance;
				expectedChance = 0;
			}
		}
		sql.replace(sql.length() - 1, sql.length(), ";");
		System.out.println(sql);
		map.put( "UPDATE", sql.toString() );
		System.out.println(updateMysql( map ));
		System.out.println(totalExpectedChance);
		System.out.println(totalChance);
		System.out.println(totalExpectedChance * 1.0 / totalChance);
	}

	public int setStockInfo2( Map<String, Object> map ) throws SQLException, IOException{
			String sqlPrefix = "REPLACE INTO STOCK_CAPITAL_HISTORY_DATA( STOCK_CODE, HISTORY_DATE,"
					+ "DAY_CLOSE, DAY_CHANGE, DAY_MAJOR_NET_INFLOW_AMOUNT,"
					+ "DAY_MAJOR_NET_INFLOW_RATIO, DAY_HUGE_NET_INFLOW_AMOUNT, "
					+ "DAY_HUGE_NET_INFLOW_RATIO, DAY_BIG_NET_INFLOW_AMOUNT, "
					+ "DAY_BIG_NET_INFLOW_RATIO, DAY_MIDDLE_NET_INFLOW_AMOUNT, "
					+ "DAY_MIDDLE_NET_INFLOW_RATIO, DAY_SMALL_NET_INFLOW_AMOUNT, "
					+ "DAY_SMALL_NET_INFLOW_RATIO ) VALUES";
			String sqlSuffix = ";";

			Document doc = Jsoup.connect( map.get("PREFIX").toString() +
					map.get("URLVAR").toString() + map.get("SUFFIX").toString() )
					 .ignoreContentType(true)
					 .data("query", "Java")   // �������  
					 .userAgent("I �� m jsoup") // ���� User-Agent   
					 .cookie("auth", "token") // ���� cookie   
					 .timeout(30000)           // �������ӳ�ʱʱ��  
					 .get();                 // ʹ�� POST �������� URL
//			System.out.println(doc);
			String sqlValue = generateStockInsert2( map, doc );
			if( !sqlValue.isEmpty() ){
				int count;
				map.put("UPDATE", sqlPrefix + sqlValue + sqlSuffix);
				System.out.println(sqlPrefix + sqlValue + sqlSuffix);
				count = updateMysql( map );
				if( count < 1 ){
//					System.out.println(map.get("UPDATE"));
					throw new SQLException( "����ʧ��" );
				}
				return count;
			}
			return 0;
	}
	
	//ֻ�������һ��/���������
	public String generateStockInsert2( Map<String, Object> map, Document doc ){
		String sql = " ";
		String[] tmpList = null;
		String[] tmpRecord = null;
		//data�ű�����
		String regex = "(?<=data:\\[\").*?(?=\\\"]\\})";
		Pattern pattern = Pattern.compile( regex );
		Matcher	matcher = pattern.matcher( doc.toString() );
		if(matcher.find()){
			tmpList = matcher.group().split("\",\"");
//			System.out.println(matcher.group());
			for( int i = 0; i < tmpList.length; ++i ){
				tmpRecord = tmpList[i].split(",");// ������¼����
//				System.out.println(tmpList[i]);
//				if( map.get("DATE").toString().equals(tmpRecord[0]) ){
//				if( "2017-03-17".equals(tmpRecord[0]) ){
				if( LocalDate.parse(tmpRecord[0]).isAfter(LocalDate.parse("2011-01-25"))
						&& LocalDate.parse(tmpRecord[0]).isBefore(LocalDate.now())){
					System.out.println( map.get("DATE") + "\t" + tmpRecord[0] );
					sql += "( '"
							+ map.get("CODE").toString() + "', '"
							+ tmpRecord[0] + "', "
							+ processNum2(tmpRecord[11], "") + ", "
							+ processNum2(tmpRecord[12], "") + ", "
							+ processNum2(tmpRecord[1], "capital") + ", "
							+ processNum2(tmpRecord[2], "") + ", "
							+ processNum2(tmpRecord[3], "capital") + ", "
							+ processNum2(tmpRecord[4], "") + ", "
							+ processNum2(tmpRecord[5], "capital") + ", "
							+ processNum2(tmpRecord[6], "") + ", "
							+ processNum2(tmpRecord[7], "capital") + ", "
							+ processNum2(tmpRecord[8], "") + ", "
							+ processNum2(tmpRecord[9], "capital") + ", "
							+ processNum2(tmpRecord[10], "") + "),";	
				}
			}
			return sql.substring(0, sql.length() - 1);			
		}else{
			return "";
		}
	}
	
	public Object processNum2( String num, String mark ){
		if("-".equals(num)){
			return 0;
		}else if("%".equals(num.substring(num.length() - 1))){
			return Float.parseFloat( num.substring( 0, num.length() - 1  )) / 100;
		}else if("capital".equals(mark)){
			return Float.parseFloat(num) * 10000;
		}else{
			return Float.parseFloat(num);
		}
	}
}

