package edu.bu.cs755;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Main {
	
	public static Stream parseLine(String article) {
		List<SimpleEntry<String, String>> list = new ArrayList<SimpleEntry<String, String>>();
		String key = getArticleKey(article);
		
		article = article.replaceAll("<[^>]+>", "").replaceAll("[^-a-zA-Z0-9]", " ").toUpperCase();
		String[] elements = article.split("\\s+");
		for (String s: elements) {  
			list.add(new SimpleEntry <> (key, s));
		}
		return list.stream().distinct(); //distinct so we don't double count same words in the same article
	}
	
	public static String getArticleKey(String article) {
		final Pattern patternPart1 = Pattern.compile("id=\"(.*?)\"");
		final Matcher matcherPart1 = patternPart1.matcher(article);
		matcherPart1.find();
		String part1 = matcherPart1.group(1);
		
		final Pattern patternPart2 = Pattern.compile("title=\"(.*?)\"");
		final Matcher matcherPart2 = patternPart2.matcher(article);
		matcherPart2.find();
		String part2 = matcherPart2.group(1);
		
		return part1 + "_" + part2; //key made up of id and title for output display purposes
	}

	public static void main(String[] args) {
		String bucket_name= "metcs755";
		String key_name="WikipediaPages_oneDocPerLine.txt"; 		
		int wordLimit = 5000; //only need top 5000 words
		int articleLimit = 20; // only need top 20 wiki pages
		
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion("us-east-1").build();

		try {
		    // TASK 1
			S3Object s3objectWord = s3Client.getObject(bucket_name, key_name);
			S3ObjectInputStream s3isWord = s3objectWord.getObjectContent();		
		    BufferedReader wikiWordReader = new BufferedReader(new InputStreamReader(s3isWord));
//			BufferedReader wikiWordReader = new BufferedReader(new FileReader("/Users/nitin.deshpande/Documents/Course/Assignments/1/WikipediaPages_oneDocPerLine_1000Lines_small.txt"));

		    System.out.println("Scanning " + key_name + " for 5000 most frquently used words and top 20 articles with the most of the top 5000 frequently used words" + "\n");
		    
//		    Supplier<Stream<String>> streamSupplier = () -> Stream.of("d2", "a2", "b1", "b3", "c")
//		    														.filter(s -> s.startsWith("a"));
//		    streamSupplier.get().anyMatch(s -> true); // ok
//		    streamSupplier.get().noneMatch(s -> true); // ok
		    		
			Map<String, Long> wordCount = wikiWordReader.lines()
														.map(l -> l.replaceAll("<[^>]+>", "").replaceAll("[^-a-zA-Z0-9]", " ").toUpperCase())
														.flatMap(l -> Arrays.stream(l.split("\\s+")))
														.map(word -> new SimpleEntry <> (word, 1))
														.collect(Collectors.groupingBy(SimpleEntry::getKey, Collectors.counting()));
			
			Map<String, Long> topWords = wordCount.entrySet().stream()
												.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
												.limit(wordLimit)
												.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));


			System.out.println("Here is the frequency position for \"during\", \"and\", \"time\", \"protein\", \"car\":");
			if(topWords.get("DURING") != null) System.out.println("Frequency position of \"during\" : " + topWords.get("DURING"));
			else System.out.println("Frequency position of \"during\" : " + -1);

			if(topWords.get("AND") != null) System.out.println("Frequency position of \"and\" : " + topWords.get("AND"));
			else System.out.println("Frequency position of \"and\" : " + -1);
			
			if(topWords.get("TIME") != null) System.out.println("Frequency position of \"time\" : " + topWords.get("TIME"));
			else System.out.println("Frequency position of \"time\" : " + -1);
			
			if(topWords.get("PROTEIN") != null) System.out.println("Frequency position of \"protein\" : " + topWords.get("PROTEIN"));
			else System.out.println("Frequency position of \"protein\" : " + -1);

			if(topWords.get("CAR") != null) System.out.println("Frequency position of \"car\" : " + topWords.get("CAR") + "\n");
			else System.out.println("Frequency position of \"car\" : " + -1 + "\n");

			// TASK 2
			S3Object s3objectArticle = s3Client.getObject(bucket_name, key_name);
			S3ObjectInputStream s3isArticle = s3objectArticle.getObjectContent();		
		    BufferedReader wikiArticleReader = new BufferedReader(new InputStreamReader(s3isArticle));
//			BufferedReader wikiArticleReader = new BufferedReader(new FileReader("/Users/nitin.deshpande/Documents/Course/Assignments/1/WikipediaPages_oneDocPerLine_1000Lines_small.txt"));
			
			Map<String, Long> articleCount = (Map<String, Long>) 
												wikiArticleReader.lines()
																.flatMap(l -> parseLine(l))
																.filter(tuple -> topWords.containsKey(((SimpleEntry)tuple).getValue()))
																.collect(Collectors.groupingBy(SimpleEntry<String,Long>::getKey, Collectors.counting()));
			
			Map<String, Long> topArticles = articleCount.entrySet().stream()
																	.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
																	.limit(articleLimit)
																	.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
										
			System.out.println("Top 20 articles with the most of the top 5000 frequently used words: ");
			topArticles.forEach((k, v) -> System.out.println(("Rank:" + v + ", ID: " + k.split("_")[0] + ", Title: " + k.split("_")[1])));
			
			s3isWord.close();
			s3isArticle.close();
									
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
