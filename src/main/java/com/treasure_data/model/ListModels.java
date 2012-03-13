package com.treasure_data.model;

import java.util.List;

interface ListModels<T extends Model> extends Model {

    List<T> getList();

    T get(String name);

    int size();
}
