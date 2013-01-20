//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.model;

import java.util.List;

public abstract class AbstractListModels<T extends AbstractModel>
        extends AbstractModel implements ListModels<T> {

    private List<T> list;

    protected AbstractListModels(List<T> list) {
        super();
        this.list = list;
    }

    /**
     * @see com.treasure_data.data.model.AbstractModel#getName()
     */
    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    public List<T> get() {
        return list;
    }

    public T get(String name) {
        for (T t : list) {
            if (t.getName().equals(name)) {
                return t;
            }
        }
        return null;
    }

    public boolean delete(String name) {
        int j = -1;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getName().equals(name)) {
                j = i;
                break;
            }
        }

        if (j != -1) {
            list.remove(j);
            return true;
        } else {
            return false;
        }
    }

    public int size() {
        return list.size();
    }
}
