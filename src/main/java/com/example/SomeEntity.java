package com.example;

public class SomeEntity {
  private Long id;

  private String svalue;

  public Long getId() { return id; }
  public void setId(Long id) { this.id = id; }

  public String getSvalue() { return svalue; }
  public void setSvalue(String svalue) { this.svalue = svalue; }

  // If 'this' (the payload) has no value, fallback to existing.
  // Otherwise, keep 'this' value to perform the update.
  public SomeEntity merge(SomeEntity existingEntity) {
    if (this.svalue == null) {
      this.svalue = existingEntity.getSvalue();
    }
    return this;
  }

  @Override
  public String toString() {
    return "SomeEntity{" +
        "id=" + id +
        ", svalue='" + svalue + '\'' +
        '}';
  }
}