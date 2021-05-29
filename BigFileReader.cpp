#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include <vector>
#include <sys/mman.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <cctype>
#include <map>
#include <set>
using namespace std;

pthread_barrier_t b;

#define MB (1024*1024)

int fd1, fd2, fd3;
void *p, *p1;

struct SPAN
{
    long start, end;
    SPAN(long a, long b)
    {
        start = a;
        end = b;
    }
    SPAN ()
    {

    }
};

struct Info
{
    //int vehicleId, frameId;
    double localX, localY ,vel;
//    int preceding, following;

    Info(){}

    Info(/*int a, int b,*/float g,  float c, float d/*, int e, int f*/)
    {
        //vehicleId = a;
        //frameId = b;
        localX = g;
        localY = c;
        vel = d;
//        preceding = e;
//        following = f;
    }

};


struct Key
{
    int vehicleId;
    long long global_time;

    bool operator < (Key k) const
    {
        return (vehicleId < k.vehicleId) || (vehicleId == k.vehicleId &&  global_time< k.global_time);
    }

    Key(int a, long long b)
    {
        vehicleId = a;
        global_time = b;
    }
};

vector<SPAN> spans;

struct MAP
{
    map<Key, Info> m;
    pthread_mutex_t lock;
    int count ;
//    set<Key> s;
//    vector<Key> v;
    MAP()
    {
        count = 0;
        m.clear();
        lock = PTHREAD_MUTEX_INITIALIZER;
    }
};

MAP mm;

void initspan(int start, int filesize, int fd)
{
    if(start > filesize - 1) return;
    SPAN s;
    s.start = start;
    long long endPosition = start + MB - 1;
    if(endPosition >= filesize - 1)
    {
        s.end = filesize - 1;
        spans.push_back(s);
//        cout << s.start <<  "end" << s.end << " "<< filesize;
        return;
    }
    if(lseek(fd, endPosition, SEEK_SET) < 0)
    {
        perror("lseek error");
        exit(1);
    }
    char c;
    while(read(fd, &c , 1) == 1 && c != '\n')
    {
        endPosition++;
        if(endPosition >= filesize - 1)
        {
            endPosition = filesize - 1;
            break;
        }
    }
    s.end = endPosition;
//    cout << s.start << "  " << s.end << "\n";
    spans.push_back(s);

    initspan(endPosition + 1, filesize, fd);
}


//solve file
void * thr_fn (void *arg)
{
    SPAN *span = (SPAN*) arg;
    //cout << span->start << " end " << span->end << "\n";

    //long long size = span->end - span->start + 1;

//    cout << "mulei" << "\n";
    char *ptr = (char *)p;

    long long pos = span->start;
//    cout <<"lala:" << ptr[pos] << "\n";

    while(pos <= span->end)
    {
        int vehicle_id = 0, frame_id = 0;
        double local_x = 0.0, local_y = 0.0, vel = 0.0;
        int preceding = 0, following = 0;
        long long global_time = 0;

        //solve vehicle_id
        while(isdigit(ptr[pos]))
        {
            vehicle_id  = vehicle_id * 10 + (ptr[pos] - '0');
            pos++;
        }

        //cout << "vehicle_id  " << vehicle_id << "  " << "\n";

        //solve frame_id
        pos++;
        while(isdigit(ptr[pos]))
        {
            frame_id = frame_id * 10 + (ptr[pos] - '0');
            pos++;
        }

        //cout << "frameId  " << frame_id << "  \n";

        //skip total_frames
        pos++;
        bool has = false;
        if(ptr[pos] == '"')
        {
            has = true;
            pos++;
        }

        if(has)
        {
            while(ptr[pos] != '"')
            {
                pos++;
            }
            pos++;
        }
        else
        {
            while(isdigit(ptr[pos]))
            {
                pos++;
            }
        }


        //read global_time
        pos++;
        while(isdigit(ptr[pos]))
        {
            global_time = global_time * 10 + (ptr[pos] - '0');
            pos++;
        }

        //cout <<"global_time  "  <<  global_time << "  \n";

        //solve local_x;
        pos++;
        while(isdigit(ptr[pos]))
        {
            local_x = local_x * 10 + (ptr[pos] - '0');
            pos++;
        }
        if(ptr[pos] == '.')
        {
            float k  = 1.0;
            pos++;
            while(ptr[pos] != ',')
            {
                k  = k * 0.1;
                local_x = local_x + k * (ptr[pos] - '0');
                pos++;
            }
        }

        //cout << frame_id << "  " << "\n";
        //solve local_y
        has = false;
        pos++;
        if(ptr[pos] == '"')
        {
            has = true;
            pos++;
        }

        if(has)
        {
            while(ptr[pos] != '"')
            {
                while(ptr[pos] != '"' && ptr[pos] != '.')
                {
                    if(ptr[pos] == ',')
                    {
                        pos++;
                        continue;
                    }
                    if(isdigit(ptr[pos]))
                    {
                        local_y = local_y * 10.0 + (ptr[pos] - '0');
                        pos++;
                    }

                }
                if(ptr[pos] == '.')
                {
                    pos++;
                    float k = 1.0;
                    while(isdigit(ptr[pos]))
                    {
                        k = k * 0.1;
                        local_y = local_y + (ptr[pos] - '0') * k;
                        pos++;
                    }
                }
            }
            pos++;// point to ,
        }
        else
        {
            while(isdigit(ptr[pos]))
            {
                local_y = local_y * 10.0 + (ptr[pos] - '0');
                pos++;
            }

            if(ptr[pos] == '.')
            {
                pos++;
                float k = 1.0;
                while(isdigit(ptr[pos]))
                {
                    k = k * 0.1;
                    local_y = local_y + (ptr[pos] - '0') * k;
                    pos ++;
                }
            }
        }

        //cout << local_y << "\n";

        //solve vel
        pos++;
        while(isdigit(ptr[pos]))
        {
            vel = vel * 10.0 + (ptr[pos] - '0');
            pos++;
        }

        if(ptr[pos] == '.')
        {
            pos++;
            float k = 1.0;
            while(isdigit(ptr[pos]))
            {
                k = k * 0.1;
                vel = vel + (ptr[pos] - '0') * k;
                pos ++;
            }
        }

        //cout << vel << "\n";

        //solve pre
        has = false;
        pos++;
        if(ptr[pos] == '"')
        {
            has = true;
            pos++;
        }
        if(has)
        {

            while(ptr[pos] != '"')
            {
                if(ptr[pos] == ',')
                {
                    pos++;
                    continue;
                }
                if(isdigit(ptr[pos]))
                {
                    preceding = preceding * 10 + (ptr[pos] - '0');
                    pos++;
                }
            }
            pos++;
        }
        else
        {
            while(isdigit(ptr[pos]))
            {
                preceding = preceding * 10 + (ptr[pos] - '0');
                pos++;
            }
        }

        //cout << preceding <<"\n";

        //solve nexe
        has = false;
        pos++;
        if(ptr[pos] == '"')
        {
            has = true;
            pos++;
        }

        if(has)
        {
            while(ptr[pos] != '"')
            {
                if(ptr[pos] == ',')
                {
                    pos++;
                    continue;
                }
                if(isdigit(ptr[pos]))
                {
                    following = following * 10 + (ptr[pos] - '0');
                    pos++;
                }
            }
            pos++;
        }
        else
        {
            while(isdigit(ptr[pos]))
            {
                following = following * 10 + (ptr[pos] - '0');
                pos++;
            }
        }
//        cout << following << "\n";
        while(ptr[pos] == '\r' || ptr[pos] == '\n') pos++;


        pthread_mutex_lock(&mm.lock);

        mm.m[Key(vehicle_id, global_time)] = Info(local_x,local_y, vel/*, preceding, following*/);
        mm.count ++;
        //mm.v.push_back(Key(vehicle_id, frame_id));
        //mm.s.insert(Key(vehicle_id, frame_id));
        //cout << vehicle_id << " " << global_time << "  "<<local_x << "  "  <<local_y << "  " << vel << "  "<<preceding << "  " << following<<"\n";
        pthread_mutex_unlock(&mm.lock);

    }
    pthread_barrier_wait(&b);
//    cout << "over\n";
    return (void *)0;
}


//-------------------------------------------------------


struct Info2
{
    int vehicleId;
    long long global_time;
    float local_x, local_y, vel;
    int laneId, preceding, following;

    Info2(int a, long long b, float c, float d, float e, int f, int g, int h)
    {
        vehicleId = a; global_time = b;
        local_x = c; local_y = d; vel = e;
        laneId = f; preceding = g; following = h;
    }
    Info2()
    {

    }

};

vector<Info2> vv;
void solve(long long size)
{
    char *ptr = (char *)p1;
    long long pos = 0;

    int vehicle = -1, lane = -1, change = -1;

    while(pos < size)
    {
        int vehicle_id = 0, lane_id = 0, preceding = 0, following = 0;
        long long global_time = 0;
        float local_x = 0.0, local_y = 0.0, vel = 0.0;

        while(isdigit(ptr[pos]))
        {
            vehicle_id = vehicle_id*10 + (ptr[pos] - '0') ;
            pos++;
        }

        //skip frame_id
        pos++;
        while(isdigit(ptr[pos]))
        {
            pos++;
        }
        //read global_time
        pos++;
        while(isdigit(ptr[pos]))
        {
            global_time = global_time * 10 + (ptr[pos] - '0');
            pos++;
        }
        while(isspace(ptr[pos])) pos++;

        //read local_X
        pos++;
        while(isdigit(ptr[pos]))
        {
            local_x = local_x * 10 + (ptr[pos] - '0');
            pos++;
        }

        if(ptr[pos] == '.')
        {
            pos++;
            float k = 1.0;
            while(isdigit(ptr[pos]))
            {
                k = k * 0.1;
                local_x += (k * (ptr[pos] - '0'));
                pos++;
            }
        }

        while(isspace(ptr[pos])) pos++;

        //read local_y
        pos++;
        while(isdigit(ptr[pos]))
        {
            local_y = local_y * 10 + (ptr[pos] - '0');
            pos++;
        }

        if(ptr[pos] == '.')
        {
            pos++;
            float k = 1.0;
            while(isdigit(ptr[pos]))
            {
                k *= 0.1;
                local_y = local_y + k * (ptr[pos] - '0');
                pos++;
            }
        }

        while(isspace(ptr[pos])) pos++;

        //read vel
        pos++;
        while(isdigit(ptr[pos]))
        {
            vel = vel * 10 + (ptr[pos] - '0');
            pos++;
        }

        if(ptr[pos] == '.')
        {
            pos++;
            float k = 1.0;
            while(isdigit(ptr[pos]))
            {
                k *= 0.1;
                vel = vel + k * (ptr[pos] - '0');
                pos++;
            }
        }

        //read lane id
        pos++;
        while(isdigit(ptr[pos]))
        {
            lane_id = lane_id * 10 + (ptr[pos] - '0');
            pos++;
        }


        //read preceding
        pos++;
        while(isdigit(ptr[pos]))
        {
            preceding = preceding * 10 + (ptr[pos] - '0');
            pos++;
        }

        while(isspace(ptr[pos])) pos++;

        //read following
        pos++;
        while (isdigit(ptr[pos]))
        {
            following = following * 10 + (ptr[pos] - '0');
            pos++;
        }


        while( isspace(ptr[pos]) ||  ptr[pos] == '\r' || ptr[pos] == '\n') pos++;
        //cout << vehicle_id << " " << global_time << "  " << local_x << " " << local_y << " " << vel <<" " << lane_id << " "<<preceding << " " << following<<"\n";
//        return;

        // solve data

        if(vehicle == -1 && lane == -1)
        {
            vehicle = vehicle_id;
            lane = lane_id;
            vv.push_back(Info2(vehicle_id, global_time, local_x, local_y, vel, lane_id, preceding, following));
        }
        else
        {
            //same car, same lane
            if(vehicle == vehicle_id && lane == lane_id)
            {
                vv.push_back(Info2(vehicle_id, global_time, local_x, local_y, vel, lane_id, preceding, following));
            }
            //same car, different lane
            else if(vehicle == vehicle_id && lane != lane_id)
            {
                change = vv.size();
                vv.push_back(Info2(vehicle_id, global_time, local_x, local_y, vel, lane_id, preceding, following));
                lane = lane_id;
            }
            //car change
            else if(vehicle != vehicle_id)
            {
                int start = 0, end = vv.size();
                //calculate data
                if(change - 5 <= 0)
                {
                    start  =  0;
                }
                else
                {
                    start = change - 5;
                }

                end = start + 9 > vv.size() -1 ? vv.size()-1 : start + 9;

                for(int i = start; i <= end; i++)
                {
                    Info2 target = vv[i], target2;
                    int pre, next, pre2, next2;
                    long long time1, time2;

                    if(i < change)
                    {
                        pre = target.preceding, next = target.following;
                        time1 = target.global_time;
                        //change lane
                        target2 = vv[change];
                        pre2 = target2.preceding, next2 = target2.following;
                        time2 = target2.global_time;
                    }
                    else
                    {
                        pre = target.preceding, next = target.following;
                        time1 = target.global_time;

                        target2 = vv[change-1];
                        pre2 = target2.preceding, next2 = target2.following;
                        time2 = target2.global_time;
                    }


                    dprintf(fd3, "%d,%lld,", target.vehicleId, target.global_time);
                    //1
                    if(pre == 0)//0
                    {
                        dprintf(fd3, "Null,Null,Null,Null,");
                    }
                    else if(mm.m.count(Key(pre, time1)) == 1)
                    {
                        Info cmp = mm.m[Key(pre, time1)];
                        double x = fabs(cmp.localX - target.local_x), y = fabs(cmp.localY - target.local_y);
                        double dis = sqrt(x * x + y * y);
                        dprintf(fd3, "%lf,%lf,%lf,%lf,", x, y, dis, target.vel - cmp.vel );
                    }
                    else //not found
                    {
                        dprintf(fd3, "null,null,null,null,");
                    }

                    //2
                    if(next == 0)
                    {
                        dprintf(fd3, "Null,Null,Null,Null,");
                    }
                    else if(mm.m.count(Key(next, time1)) == 1)
                    {
                        Info cmp = mm.m[Key(next, time1)];
                        double x = fabs(cmp.localX - target.local_x), y = fabs(cmp.localY - target.local_y);
                        double dis = sqrt(x *x + y * y);
                        dprintf(fd3, "%lf,%lf,%lf,%lf,", x, y, dis, target.vel - cmp.vel );
                    }
                    else
                    {
                        dprintf(fd3, "null,null,null,null,");
                    }
                    //------------------------------------------------------
                    //3
                    if(pre2 == 0)
                    {
                        dprintf(fd3, "Null,Null,Null,Null,");
                    }
                    else if(mm.m.count(Key(pre2, time2)) == 1)
                    {
                        Info cmp = mm.m[Key(pre2, time2)];
                        double x = fabs(cmp.localX - target.local_x), y = fabs(cmp.localY - target.local_y);
                        double dis = sqrt(x *x + y * y);
                        dprintf(fd3, "%lf,%lf,%lf,%lf,", x, y, dis, target.vel - cmp.vel);
                    }
                    else
                    {
                        dprintf(fd3, "null,null,null,null,");
                    }

                    //4
                    if(next2 == 0)
                    {
                        dprintf(fd3, "Null,Null,Null,Null\n");
                    }
                    else if(mm.m.count(Key(next2, time2)) == 1)
                    {
                        Info cmp = mm.m[Key(next2, time2)];
                        double x = fabs(cmp.localX - target.local_x), y = fabs(cmp.localY - target.local_y);
                        double dis = sqrt(x *x + y * y);
                        dprintf(fd3, "%lf,%lf,%lf,%lf\n", x, y, dis, target.vel - cmp.vel);
                    }
                    else
                    {
                        dprintf(fd3, "null,null,null,null\n");
                    }
                }


                //-----------------------------------
                vv.clear();
                vehicle = vehicle_id;
                lane = lane_id;
                vv.push_back(Info2(vehicle_id, global_time, local_x, local_y, vel, lane_id, preceding, following));
            }
        }
    }
}



int main()
{
//    printf("hello world\n");
    freopen("1.txt", "w", stdout);

    pthread_t tid;
    fd1 = open("../1.csv", O_RDONLY);
//    fd1 = open("1.txt", O_RDONLY);
    //file size
    struct stat statbuf;
    if(fstat(fd1, &statbuf) < 0)
    {
        perror("1.csv open error");
        exit(1);
    }
    long long f1size = statbuf.st_size;

    //file 2 time
    time_t start = time(NULL);

    initspan(0, f1size, fd1);
    p = mmap(NULL, f1size, PROT_READ | PROT_WRITE, MAP_PRIVATE ,fd1, 0);
    if(MAP_FAILED == p)
    {
        //cout << "mmap failed\n";
        perror("mmap failed");
        exit(1);
    }

    pthread_barrier_init(&b, NULL, spans.size() + 1);

//    cout << "thread cout: " << spans.size() << "\n";
    printf("thread count: %ld\n", spans.size());
    fflush(stdout);

    for(int i = 0; i < spans.size(); i++)
    {
        int err = pthread_create(&tid, NULL, thr_fn, (void *)&spans[i]);
        if(err < 0)
        {
            perror("can't create thread");
            exit(1);
        }
    }

    pthread_barrier_wait(&b);

    if(munmap(p, f1size) < 0)
    {
        perror("munmap error");
        exit(1);
    }


//    cout << "map size:" << mm.m.size() <<" "<<mm.count <<"\n";
    printf("map size: %ld, %d\n", mm.m.size(), mm.count);
    fflush(stdout);
    // read 2.csv
//---------------------------------------------------------
    fd2 = open("../2.csv", O_RDONLY);
    if(fd2 < 0)
    {
        perror("open 2 failed");
        exit(1);
    }

    fd3  = open("../3.csv", O_WRONLY | O_CREAT);
    if(fd3 < 0)
    {
        perror("create error");
        exit(1);
    }
    dprintf(fd3, "Vehicle_ID,Global_Time,delta1p_X,delta1p_Y,delta1p_Dis,delta1p_Vel,delta2f_X,delta2f_Y,delta2f_Dis,delta2f_Vel,"
                 "delta3p_X,delta3p_Y,delta3p_Dis,delta3p_Vel,delta4f_X,delta4f_Y,delta4f_Dis,delta4f_Vel\n");

//    struct stat statbuf;
    if(fstat(fd2, &statbuf) < 0)
    {
        perror("2.csv open error");
        exit(1);
    }
    long long f2size = statbuf.st_size;

    p1 = mmap(NULL, f2size, PROT_READ, MAP_PRIVATE, fd2, 0);
    if(p1 == MAP_FAILED)
    {
        perror("map fd2 error");
        exit(1);
    }
    solve(f2size);

    munmap(p1, f2size);

    time_t end = time(NULL);
//    cout << "total time:" <<end - start << "\n";
    printf("total time: %ld\n", end-start);
    fflush(stdout);
    return 0;
}
