package io.scalecube.ipc;

import java.util.Objects;

public final class Qualifier {

  public static final String DELIMITER = "/";
  public static final String NAMESPACE = "io.scalecube.ipc";
  public static final String ERROR_NAMESPACE = NAMESPACE + ".error";

  // qualifier for generic error
  public static final Qualifier Q_GENERAL_FAILURE = Qualifier.fromString(ERROR_NAMESPACE + DELIMITER + 500);
  // qualifier for onCompleted event
  public static final Qualifier Q_ON_COMPLETED = Qualifier.fromString(NAMESPACE + "/onCompleted");

  private final String namespace;
  private final String action;
  private final String stringValue; // calculated

  /**
   * Basic constructor with namespace and action.
   */
  public Qualifier(String namespace, String action) {
    this.namespace = namespace;

    this.action = action;
    if (action == null) {
      this.stringValue = namespace;
    } else {
      this.stringValue = namespace + DELIMITER + action;
    }
  }

  public String getNamespace() {
    return namespace;
  }

  public String getAction() {
    return action;
  }

  public String asString() {
    return stringValue;
  }

  public boolean equals(String qualifier) {
    return stringValue.equals(qualifier);
  }

  public boolean equalsIgnoreCase(String qualifier) {
    return stringValue.equalsIgnoreCase(qualifier);
  }

  /**
   * Constructs qualifier object from string.
   */
  public static Qualifier fromString(String qualifierAsString) throws IllegalArgumentException {
    int indexOf = qualifierAsString.indexOf(DELIMITER);
    if (indexOf == -1) {
      // whole string is namespace
      return new Qualifier(qualifierAsString, null);
    }
    if (indexOf + 1 >= qualifierAsString.length()) {
      String namespace = qualifierAsString.substring(0, indexOf);
      if (namespace.isEmpty()) {
        throw new IllegalArgumentException(qualifierAsString);
      }
      return new Qualifier(namespace, null);
    }
    String namespace = qualifierAsString.substring(0, indexOf);
    String action = qualifierAsString.substring(indexOf + 1);
    return new Qualifier(namespace, action);
  }

  /**
   * @return qualifier namespace.
   */
  public static String getQualifierNamespace(String qualifierAsString) {
    int pos = qualifierAsString.indexOf(DELIMITER);
    if (pos == -1) {
      return qualifierAsString;
    }
    return qualifierAsString.substring(0, pos);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Qualifier qualifier = (Qualifier) obj;

    return Objects.equals(stringValue, qualifier.stringValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stringValue);
  }

  @Override
  public String toString() {
    return stringValue;
  }
}
