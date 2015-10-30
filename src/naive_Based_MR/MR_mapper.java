package naive_Based_MR;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class MR_mapper extends
Mapper<LongWritable, Text, IntWritable, Text>{
	

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if(value!=null) {
		
			String dimTemp[]=value.toString().split("\t");
			
			System.out.println(value.toString());
		
			if (dimTemp.length >= 22) {
				int adv_id=Integer.parseInt(dimTemp[22]);
				
				StringBuilder sb=new  StringBuilder();
				
				sb.append(dimTemp[6]+"\t");
				sb.append(dimTemp[16]+"\t");
				if(dimTemp.length>23)
					sb.append(dimTemp[23]+"\t");
				else
					sb.append("null"+"\t");
				
				sb.append(dimTemp[0]+"\t");
				
				Text outValue=new Text(sb.toString().trim());
				
				context.write(new IntWritable(adv_id),outValue);
			}
		}
		
	}

}
