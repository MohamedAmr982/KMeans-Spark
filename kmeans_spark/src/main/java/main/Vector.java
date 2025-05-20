package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Vector implements Serializable {

    public float[] elements;

    public Vector(int length) {
        this.elements = new float[length];
    }

    public Vector(float[] elements) {
        this.elements = elements;
    }

    public Vector(int length, String vecStr, String delimiter) {
        this.elements = new float[length];
        String[] strElements = vecStr.split(delimiter);
        for (int i = 0; i < length; i++) {
            this.elements[i] = Float.parseFloat(strElements[i]);
        }
    }


    // TODO: implement custom (de)serialization

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(elements.length);
        for (int i = 0; i < elements.length; i++) {
            out.writeFloat(elements[i]);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException {
        int length = in.readInt();
        this.elements = new float[length];
        for (int i = 0; i < length; i++) {
            this.elements[i] = in.readFloat();
        }
    }

    public float squaredDistanceTo(Vector v) {
        float sum = 0;
        for (int i = 0; i < elements.length; i++) {
            float diff = elements[i] - v.elements[i];
            sum += diff * diff;
        }
        return sum;
    }

    public Vector add(Vector v) {
        Vector sum = new Vector(elements.length);
        for (int i = 0; i < elements.length; i++) {
            sum.elements[i] = v.elements[i] + this.elements[i];
        }
        return sum;
    }

    public Vector divideBy(float f) {
        Vector v = new Vector(elements.length);
        for (int i = 0; i < elements.length; i++) {
            v.elements[i] = this.elements[i] / f;
        }
        return v;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < elements.length; i++) {
            builder.append(String.valueOf(elements[i]));
            if (i != elements.length - 1) {
                builder.append(",");
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
