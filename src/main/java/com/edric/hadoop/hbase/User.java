package com.edric.hadoop.hbase;

public class User {
    public String id;
    public String name;
    public String email;
    public String password;

    public String toString() {
        return String.format("<User: %s, %s, %s>", id, name, email);
    }
}
