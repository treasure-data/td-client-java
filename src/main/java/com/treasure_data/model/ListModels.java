package com.treasure_data.model;

import java.util.List;

interface ListModels<T extends Model> extends Model {

    List<T> get();

    T get(String name);

    int size();

    boolean delete(String name);
}
