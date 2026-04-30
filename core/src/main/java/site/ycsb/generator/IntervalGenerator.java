package site.ycsb.generator;

import java.util.Random;

/**
 * Java implementation of the IntervalGenerator for YCSB.
 * Overrides the standard YCSB NumberGenerator methods to handle interval-based generation.
 */
public class IntervalGenerator extends NumberGenerator {

  private final Random random;
  private final long distance;
  private final long stepSize;
  private final long max;

  public IntervalGenerator(long steps, long distance, long max) {
    this.random = new Random();
    this.distance = distance;
    this.stepSize = steps;
    this.max = max;
    nextValue(0); // Initialize lastInt
  }

  /**
   * Generates the next value based on a specific transaction ID.
   * This overrides the base class implementation.
   */
  @Override
  public synchronized Long nextValue(long txnId) {
    long offset = (distance > 0) ? (random.nextLong() % distance) : 0;
    if (offset < 0) {
      offset += distance; // Correct for negative results of % operator
    }

    long ret = ((txnId * stepSize) + offset) % max;
    setLastValue(ret);
    return ret;
  }

  @Override
  public Long nextValue() {
    return nextValue(0);
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
  }
}