package com.tw.pricex;


import org.json.JSONObject;

import java.io.Serializable;

public class Product implements Serializable {
    String name;
    double price;
    String dateTime;

    public Product(String product) {
        JSONObject obj = new JSONObject(product);
        this.name = obj.getString("name");
        this.price = obj.getDouble("price");
        this.dateTime = obj.getString("date_time");
    }

    public String toJSON() {
        return "{ \n" +
                "\t\"name\": \"" + name + "\",\n" +
                "\t\"price\": \"" + price + "\",\n" +
                "\t\"date_time\":\"" + dateTime + "\"\n" +
                "}";
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", price=" + price +
                ", dateTime=" + dateTime +
                '}';
    }

    public static void main(String[] args) {
        Product p = new Product("{ \n" +
                "\t\"name\": \"Toy\",\n" +
                "\t\"price\": \"25478.0\",\n" +
                "\t\"date_time\":\"2020-10-22T14:30:58.358704\"\n" +
                "}");
        System.out.println(p.toJSON());
    }
}
