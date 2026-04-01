package com.example;

import java.io.Serializable;

public class Person implements Serializable {

    private int id;
    private String name;

    // Constructor
    public Person() {}

    public Person(int id, String name) {
        this.id = id;
        this.name = name;
    }

    // Getter & Setter
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}