import java.util.*;

public class Fridge {

  private ArrayList<String> shelf_1 = new ArrayList<String>();
  private ArrayList<String> shelf_2 = new ArrayList<String>();
  private ArrayList<String> shelf_3 = new ArrayList<String>();
  private Map<Integer, ArrayList> Shelves = new HashMap<Integer, ArrayList>() {{
  		put(100, shelf_1);
  		put(50, shelf_2);
  		put(25, shelf_3);
  }};
  private Map<String, Integer> Items = new HashMap<String, Integer>() {{
        put("ketchup", 5);
        put("mustard", 5);
        put("milk", 15);
        put("chicken", 20);
        put("bananas", 18);
    }};

  void putObject(String thing, Integer size) {
    if (Items.containsKey(thing)) {
    	if (Shelves.containsKey(size)) {
    		Shelves.get(size).add(thing)
    		System.out.println(Shelves.get(size));
    	} else {
    		System.out.println("Unknown Shelf.");
    	}

    } else {
    	System.out.println("Unknown Item.");
    }
  }

  void removeObject(String thing, Integer size) {
    if (Items.containsKey(thing)) {
    	if (Shelves.containsKey(size)) {

    	} else {
    		System.out.println("Unknown Shelf");
    	}

    } else {
    	System.out.println("Unknown Item.");
    }
  }
}
