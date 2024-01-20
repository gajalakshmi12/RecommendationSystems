import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleYouMightKnowPartIVB {
	
	static HashMap<Integer, ArrayList<Integer>> userFriends = new HashMap<>();
	static ArrayList<ArrayList<Integer>> recommendedFriends = new ArrayList<>();
	static int userCounter = 0;
	static int totalUsers = 0;
	
	public static class PeopleYouMightKnowMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text userId = new Text();
		private Text friendsList = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String[] friends = new String[itr.countTokens()];
			int index = 0;
			/* Parsing String Tokens and storing them in a String Array */
			while (itr.hasMoreTokens()) {
				friends[index++] = itr.nextToken();
			}
			 
			userId.set(friends[0]); 
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < friends.length; i++) {				
				sb.append(friends[i].toString());							
			}
			
			friendsList.set(sb.toString().trim());
			context.write(userId, friendsList);
			totalUsers++;
		}

	}

	public static class PeopleYouMightKnowReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			/*
			 * Store user and list of friends in a hashmap with key as userId
			 * and value as an arrayList of friends
			 */
			ArrayList<Integer> listofFriends = new ArrayList<>();

			int inpKey = Integer.parseInt(key.toString());

			for (Text val : values) {
				String valuesArray[] = val.toString().split(",");
				for (String value : valuesArray) {
					if (value.isEmpty() || value == "")
						listofFriends.add(-1);
					else
						listofFriends.add(Integer.parseInt(value));
				}

			}

			// Store values in HashMap
			userFriends.put(inpKey, listofFriends);
			userCounter++;
            System.out.println("User Counter: "+userCounter);
			if (userCounter == totalUsers) {
			
			
			/* Maintain an array List of user IDs */
			Set<Integer> listOfUsers = new TreeSet<>(userFriends.keySet());

			  for(int i : listOfUsers) {
				for(int j : listOfUsers)
					if(i != j) {
					boolean checkFlag = false;
					int user1 = i;
					int user2 = j;
					double influenceScore = 0;
					ArrayList<Integer> listForUser1 = userFriends.get(user1);
					if (!(listForUser1.contains(user2))) {
						ArrayList<Integer> listForUser2 = userFriends.get(user2);
						ArrayList<Integer> mutualFriends = new ArrayList<>();
						for (int k = 0; k < listForUser1.size(); k++) {
							if (listForUser2.contains(listForUser1.get(k))) {
								int friendsSize = userFriends.get(user1).size();
								influenceScore += (1.0 / friendsSize);
							}
						}
						if (influenceScore > 0) {
						    mutualFriends.add(user1); // contains User 1
						    mutualFriends.add(user2); // contains User 2
						    mutualFriends.add((int) influenceScore); // contains the influence Score																
						}
						if (!recommendedFriends.isEmpty()) {
							for (int ind = 0; ind < recommendedFriends.size(); ind++) {
								ArrayList<Integer> arrayList = recommendedFriends
										.get(ind);
								if (arrayList.get(0) == user1
										&& arrayList.get(1) == user2) {
									checkFlag = true;
								}
							}
							if (checkFlag == false && influenceScore != 0 )
								recommendedFriends.add(mutualFriends);
						} else {
							if (influenceScore != 0 )
							recommendedFriends.add(mutualFriends);
						}
					}
					
				   }

				}
			
			Collections.sort(recommendedFriends,
					new Comparator<ArrayList<Integer>>() {
						public int compare(ArrayList<Integer> list1,
								ArrayList<Integer> list2) {
							// compare user ID
							int compareFirst = list1.get(0).compareTo(
									list2.get(0));
							if (compareFirst != 0) {
								return compareFirst;
							} else {
								// compare Influence Score
								int compareThird = list2.get(2).compareTo(
										list1.get(2));
								if (compareThird != 0) {
									return compareThird;
								} else {
									return list1.get(1).compareTo(list2.get(1));
								}
							}
						}
					});
			

			// Iterate through userIDs and store recommended Friends in values

			int count = 1;
			StringBuilder sb = new StringBuilder();

			
				for (Integer i : listOfUsers) {
					String user = String.valueOf(i);
					if (userFriends.get(i).contains(-1)) {
						context.write(new Text(user), new Text(" "));
					}
					else {
					if (recommendedFriends.size() != 0) {
						for (int j = 0; j < recommendedFriends.size(); j++) {
							if(count > 10)
								break;
							ArrayList<Integer> innerList = recommendedFriends.get(j);									
							if (i.equals(innerList.get(0)) && count <= 10) {
								sb.append(innerList.get(1).toString());
								sb.append(",");
								count++;
							} else {
								if (!sb.toString().isEmpty()) {
									sb.deleteCharAt(sb.length() - 1); // remove comma at last
									context.write(new Text(user), new Text(sb.toString()));
								}
								count = 1;
								sb.setLength(0);
							}
						}
						if (!sb.toString().isEmpty()) {
							sb.deleteCharAt(sb.length() - 1); // remove comma at last
							context.write(new Text(user), new Text(sb.toString()));
						}
						count = 1;
						sb.setLength(0);
					  }
					}
				}
		     }
	      }

		}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "People You Might Know");

		job.setJarByClass(PeopleYouMightKnowPartIVB.class);

		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.setMapperClass(PeopleYouMightKnowMapper.class);
		job.setReducerClass(PeopleYouMightKnowReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

