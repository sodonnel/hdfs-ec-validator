package com.sodonnell;

import java.util.ArrayList;
import java.util.List;

public class Iterations {

  public List<List<Integer>> listCombinations(int size, int index, int take) {
    List<List<Integer>> results = new ArrayList<>();
    for (int i=index; i<size; i++) {
      if (take == 1) {
        ArrayList<Integer> val = new ArrayList<>();
        val.add(i);
        results.add(val);
      } else {
        List<List<Integer>> r = listCombinations(size, i + 1, take - 1);
        for(List<Integer> val : r) {
          List<Integer> indicies = new ArrayList<>();
          indicies.add(i);
          indicies.addAll(val);
          results.add(indicies);
        }
      }

    }
    return results;
  }

  public static void main(String[] args) {
    Iterations itr = new Iterations();
    List<List<Integer>> res = itr.listCombinations(14, 0, 10);
    for (List<Integer> r : res) {
      System.out.println(r);
    }
    System.out.println("Total combinations: "+res.size());
  }

}
