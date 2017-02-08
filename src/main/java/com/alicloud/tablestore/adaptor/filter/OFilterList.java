package com.alicloud.tablestore.adaptor.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OFilterList implements OFilter {
  /**
   * set operator
   */
  public static enum Operator {
    /**
     * !AND
     */
    MUST_PASS_ALL,
    /**
     * !OR
     */
    MUST_PASS_ONE
  }

  private Operator operator = Operator.MUST_PASS_ALL;
  private List<OFilter> filters = new ArrayList<OFilter>();

  /**
   * Default constructor, filters nothing. Required though for RPC deserialization.
   */
  public OFilterList() {
  }

  /**
   * Constructor that takes a set of {@link OFilter}s. The default operator MUST_PASS_ALL is
   * assumed.
   * @param rowFilters list of filters
   */
  public OFilterList(final List<OFilter> rowFilters) {
    this.filters = rowFilters;
  }

  /**
   * Constructor that takes a var arg number of {@link OFilter}s. The fefault operator MUST_PASS_ALL
   * is assumed.
   * @param rowFilters
   */
  public OFilterList(final OFilter... rowFilters) {
    this.filters.addAll(Arrays.asList(rowFilters));
  }

  /**
   * Constructor that takes an operator.
   * @param operator Operator to process filter set with.
   */
  public OFilterList(final Operator operator) {
    this.operator = operator;
  }

  /**
   * Constructor that takes a set of {@link OFilter}s and an operator.
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public OFilterList(final Operator operator, final List<OFilter> rowFilters) {
    this.filters = rowFilters;
    this.operator = operator;
  }

  /**
   * Constructor that takes a var arg number of {@link OFilter}s and an operator.
   * @param operator Operator to process filter set with.
   * @param rowFilters Filters to use
   */
  public OFilterList(final Operator operator, final OFilter... rowFilters) {
    this.filters = Arrays.asList(rowFilters);
    this.operator = operator;
  }

  /**
   * Get the operator.
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Get the filters.
   * @return filters
   */
  public List<OFilter> getFilters() {
    return filters;
  }

  /**
   * Add a filter.
   * @param filter another filter
   */
  public void addFilter(OFilter filter) {
    this.filters.add(filter);
  }
}
