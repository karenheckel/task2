package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.Text;

public class WordAndCount implements Comparable<WordAndCount> {

    private final Text word;
    private double delayRatio;

    public WordAndCount(Text word, double delayRatio) {
        this.word = word;
        this.delayRatio = delayRatio;
    }

    public Text getWord() {
        return word;
    }

    public double getDelayRatio() {
        return delayRatio;
    }

    /**
     * Compares two sort data objects by their value.
     * 
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override
    public int compareTo(WordAndCount other) {
        return Double.compare(this.delayRatio, other.delayRatio);
    }

    public String toString() {
        return "(" + word.toString() + " , " + Double.toString(delayRatio) + ")";
    }
}
