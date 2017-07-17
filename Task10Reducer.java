package mapreduce.task10;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task10Reducer extends Reducer<Text, SalesUnit, Text, SalesUnit>{
	
	/**
	 * The mapper outputs companyName and SalesUnit(which contains state and price information)
	 * So the input key type will be text and the input value type is SalesUnit
	 * 
	 * Now group the values by state and for each group sum the prices. If the resulting grouped list 
	 * is sorted in descending order by price and we take only the first 3 entries in the sorted list, then
	 * we will get the top 3 states for this company
	 * 
	 * The output now will be the company name and the first 3 elements in the sorted list  
	 */
	
	@Override
	protected void reduce(Text companyName, Iterable<SalesUnit> stateSales,
			Reducer<Text, SalesUnit, Text, SalesUnit>.Context context) 
					throws IOException, InterruptedException {
		
		// create a map to group records by the state and sum up the prices for each such group
		Map<String, SalesUnit> map = new HashMap<String, SalesUnit>();
		for(SalesUnit stateSale : stateSales) {
			SalesUnit salesUnitElem = new SalesUnit();
			salesUnitElem.set(stateSale.getState(), stateSale.getPrice());
			// if a record for the state in the incoming record is already in the map, 
			// take the price in the map and add the price of the incoming record to it
			if(map.containsKey(salesUnitElem.getState())) {
				SalesUnit salesUnitFromMap = map.get(salesUnitElem.getState());
				salesUnitFromMap.setPrice(salesUnitFromMap.getPrice() + salesUnitElem.getPrice());
			} else { // if the state does not exist in the map, add it to the map 
				map.put(salesUnitElem.getState(), salesUnitElem);
			}
		}
		
		// Now take the list of state grouped data and sort it in descending order. 
		// The sorting will be in descending order because of the logic inside compareTo method 
		// in SalesUnit class
		List<SalesUnit> salesUnitList = new ArrayList<SalesUnit>(map.values());
		Collections.sort(salesUnitList);
		
		// Take only top 3 entries or top n entries whichever is lesser
		int endIndex = (salesUnitList.size() >= 3) ? 3 : salesUnitList.size();
		
		// output only top 3 or top n entries whichever is lesser
		// The SalesUnit object will be output as per the toSTring() 
		// method in the SalesUnit 
		for(int i = 0;i < endIndex;i++) {
			context.write(companyName, salesUnitList.get(i));
		}
	}
}
