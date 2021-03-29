public class InputEvent {

    private int sensor_id;
    private long ts;

    private float  a, v, vx, vy, vz, x, y,z ,ax, ay, az;

    public InputEvent(int sensor_id, long ts, float x, float y, float z, float v, float a, float vx, float vy, float vz, float ax, float ay, float az) {
        this.sensor_id = sensor_id;
        this.ts = ts;
        this.x = x;
        this.y = y;
        this.z = z;
        this.v = v;
        this.vx = vx;
        this.vy = vy;
        this.vz = vz;
        this.a = a;
        this.ax = ax;
        this.ay = ay;
        this.az = az;
    }

    public int getSensor_id() {
        return sensor_id;
    }

    public long getTs() {
        return ts;
    }

    public float getX() {
        return x;
    }

    public float getY() {
        return y;
    }

    public float getZ() {
        return z;
    }

    public float getV() {
        return v;
    }

    public float getVx() {
        return vx;
    }

    public float getVy() {
        return vy;
    }

    public float getVz() {
        return vz;
    }

    public float getA() {
        return a;
    }

    public float getAx() {
        return ax;
    }

    public float getAy() {
        return ay;
    }

    public float getAz() {
        return az;
    }

    @Override
    public String toString() {
        return "InputEvent{" +
                "sensor_id=" + sensor_id + "\n" +
                ", ts=" + ts +"\n" +
                ", x=" + x +"\n" +
                ", y=" + y +"\n" +
                ", z=" + z +"\n" +
                ", v=" + v +"\n" +
                ", vx=" + vx +"\n" +
                ", vy=" + vy +"\n" +
                ", vz=" + vz +"\n" +
                ", a=" + a +"\n" +
                ", ax=" + ax +"\n" +
                ", ay=" + ay +"\n" +
                ", az=" + az +"\n" +
                '}';
    }


}
