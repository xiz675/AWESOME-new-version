package edu.sdsc.variables.logicalvariables;

import java.util.ArrayList;
import java.util.List;

public class TupleTableEntry extends VariableTableEntry{
  private Integer size;
  private List<String> types;

  public TupleTableEntry(String name, Integer... block){
    super(name, DataTypeEnum.Tuple.ordinal(), block);
  }

  public TupleTableEntry(String name, List<String> types, Integer... block){
    super(name, DataTypeEnum.Tuple.ordinal(), block);
    this.setTypes(types);
  }

  public TupleTableEntry(String name, List<String> types, Integer size, Integer... block){
    this(name,types, block);
    this.size = size;
  }

  public TupleTableEntry(String name, List<String> types, Integer size, List<Object> value, Integer... block){
    this(name,  types, size,  block);
    setValue(value);
  }

  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public void setTypes(List<String> types) {
    this.types = types;
  }

  public List<String> getTypes() {
    return types;
  }
}
