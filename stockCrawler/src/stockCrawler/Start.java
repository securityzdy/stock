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

		//��ȡ���й�Ʊ����
		//��Դ�Ƕ����Ƹ����ʽ�����ı�
//		sqlInfo.put("PREFIX", "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx/JS.aspx?type=ct&st=(BalFlowMain)&sr=-1&p=");
//		sqlInfo.put("SUFFIX", "&ps=50&js=var%20GOOGEBlw={pages:(pc),date:%222014-10-22%22,data:[(x)]}&token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITA&rt=49154253");
//		sqlInfo.put("URLVAR", 1);
//		sC.setCodeData( sqlInfo );
		
		
		//��ȡָ�����ڵ����ݣ�Ĭ��Ϊ����
		sC.setStockDailyData(sqlInfo);
		
		
		//����������С���̼ۼ���Ӧ���ڣ����������ݿ�
//		sC.setMaxMinCloseInfo(sqlInfo);
		
		//����KDJ����
//		sC.setKDJ(sqlInfo);
		
		//����MACD����
//		sC.setMACD(sqlInfo);
		
		//����ָ�꣬�趨�����ǩ
//		System.out.println( "��������:\t" + sC.setClassificationLabel( sqlInfo ) );
		
//		sC.analyzeQulification(sqlInfo);
		
//		sC.potentialChance(sqlInfo);
		
		sC.endMysqlCon(sqlInfo);
	}

}
