package stockCrawler;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Start {
	public static void main(String[] args) throws IOException, InterruptedException, SQLException, ParseException{
		StockCrawler sC = new StockCrawler();
		Map<String, Object> sqlInfo = sC.initMysqlCon();

		//获取所有股票代码
		//来源是东方财富网资金流向的表单
//		sqlInfo.put("PREFIX", "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx/JS.aspx?type=ct&st=(BalFlowMain)&sr=-1&p=");
//		sqlInfo.put("SUFFIX", "&ps=50&js=var%20GOOGEBlw={pages:(pc),date:%222014-10-22%22,data:[(x)]}&token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITA&rt=49154253");
//		sqlInfo.put("URLVAR", 1);
//		sC.setCodeData( sqlInfo );
		
		
		//爬取指定日期的数据，默认为当日
		sC.setStockDailyData(sqlInfo);
		
		
		//计算出最大最小收盘价及对应日期，并插入数据库
//		sC.setMaxMinCloseInfo(sqlInfo);
		
		//计算KDJ序列
//		sC.setKDJ(sqlInfo);
		
		//计算MACD序列
//		sC.setMACD(sqlInfo);
		
		//计算指标，设定分类标签
//		System.out.println( "覆盖条数:\t" + sC.setClassificationLabel( sqlInfo ) );
		
//		sC.analyzeQulification(sqlInfo);
		
//		sC.potentialChance(sqlInfo);
		
		sC.endMysqlCon(sqlInfo);
	}

}
