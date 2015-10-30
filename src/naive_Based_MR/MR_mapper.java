package naive_Based_MR;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MR_mapper extends
Mapper<LongWritable, Text, IntWritable, Text>{
	

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String logtype = conf.get("log_type");
		
		if(value!=null) {
			
			String dimTemp[]=value.toString().split("\t");
			//System.out.println(value.toString());
		
			if (logtype.toLowerCase().equals("bid")) {

				if (dimTemp.length >= 21) {
					int adv_id=Integer.parseInt(dimTemp[19]); // Advertiser ID
					
					StringBuilder sb=new  StringBuilder();
					
					sb.append(dimTemp[5]+"\t"); // Region
					sb.append(dimTemp[6]+"\t"); // City
					sb.append(dimTemp[18]+"\t"); // Bidding price
					
					sb.append(dimTemp[0]+"\t"); // Bid ID
					sb.append(dimTemp[1]+"\t"); // Timestamp
					
					Text outValue=new Text(sb.toString().trim());
					context.write(new IntWritable(adv_id),outValue);
				}
			} else {
				if (dimTemp.length >= 22) {
					int adv_id=Integer.parseInt(dimTemp[22]); // Advertiser ID
					
					StringBuilder sb=new  StringBuilder();
					
					sb.append(dimTemp[6]+"\t"); // Region
					sb.append(dimTemp[7]+"\t"); // City
					sb.append(dimTemp[16]+"\t");
					if(dimTemp.length>23)
						sb.append(dimTemp[23]+"\t");
					else
						sb.append("null"+"\t");
					
					sb.append(dimTemp[0]+"\t"); // Bid ID
					sb.append(dimTemp[1]+"\t"); // Timestamp
					
					Text outValue=new Text(sb.toString().trim());
					context.write(new IntWritable(adv_id),outValue);
				}
			}
		}
		
	}

}
