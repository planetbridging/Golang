package main

import (
    "fmt"
    "io/ioutil"
    "strings"
    "sync"
    "time"
    "os"
)

//STREET_LOCALITY_PID,STREET_NAME,STREET_TYPE_CODE,LOCALITY_PID,Suburb,Postcode,LONGITUDE,LATITUDE,State
type ObjLya struct {
    LOCALITY_PID string
    STREET_LOCALITY_PID[] string
    Data[] string
}

var LstLya[]string
var LstLOCALITY_PID[]string
var LstObjLya[]ObjLya
var StateCount int
var StateLength int



func contains(arr []string, str string) bool {
   for _, a := range arr {
      if a == str {
         return true
      }
   }
   return false
}

func ProcessLineLOCALITY_PID(r string, wg *sync.WaitGroup){
  defer wg.Done()
  var r_split = strings.Split(r,",")
  found := contains(LstLOCALITY_PID,r_split[3])
  if(!found){
    LstLOCALITY_PID = append(LstLOCALITY_PID, r_split[3])
    tmp_objlya := ObjLya{LOCALITY_PID: r_split[3]}
    //tmp_objlya.LOCALITY_PID = r_split[3]
    LstObjLya = append(LstObjLya, tmp_objlya)
  }
}

func SortSTREET_LOCALITY_PID(r string, wg *sync.WaitGroup){
  defer wg.Done()
  var r_split = strings.Split(r,",")
  for i := 0; i < len(LstObjLya); i++ {
    //fmt.Println(LstObjLya[i])
    if(LstObjLya[i].LOCALITY_PID == r_split[3]){
      LstObjLya[i].STREET_LOCALITY_PID = append(LstObjLya[i].STREET_LOCALITY_PID, r_split[0])
      break
    }
  }
}

func PlaceDataInSuburbID(r string, wg *sync.WaitGroup){
  defer wg.Done()
  var r_split = strings.Split(r,"|")
  if(len(r_split) > 1){
    for i := 0; i < len(LstObjLya); i++ {
      found := contains(LstObjLya[i].STREET_LOCALITY_PID,r_split[19])
      if(found){
        LstObjLya[i].Data = append(LstObjLya[i].Data, r)
        StateCount += 1
        break
      }
    }
  }
}

func SortState(filename string){
  fmt.Println("Starting: ", filename)
  start := time.Now()
  StateCount = 0
  var LstState[]string
  data, err := ioutil.ReadFile(filename)
  if err != nil {
      fmt.Println("File reading error", err)
      return
  }

  for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
    LstState = append(LstState, line)
  }

  StateLength = len(LstState)
  fmt.Println("StateSize: ", StateLength)
  var wgplace sync.WaitGroup
  for _, r := range LstState {
    wgplace.Add(1)
    go PlaceDataInSuburbID(r,&wgplace)
  }
  wgplace.Wait()
  fmt.Println("Finished: ", filename)
  t := time.Now()
  elapsed := t.Sub(start)
  fmt.Println(elapsed)
}

func WriteSuburbID(){
  for _, olya := range LstObjLya {
    if(len(olya.Data) > 0){
      f, err := os.Create("output/" + olya.LOCALITY_PID + ".txt")
      if err != nil {
          fmt.Println(err)
                  f.Close()
          return
      }
      fmt.Fprintln(f, "ADDRESS_DETAIL_PID|BUILDING_NAME|LOT_NUMBER_PREFIX|LOT_NUMBER|LOT_NUMBER_SUFFIX|FLAT_TYPE_CODE|FLAT_NUMBER_PREFIX|FLAT_NUMBER|FLAT_NUMBER_SUFFIX|LEVEL_TYPE_CODE|LEVEL_NUMBER_PREFIX|LEVEL_NUMBER|LEVEL_NUMBER_SUFFIX|NUMBER_FIRST_PREFIX|NUMBER_FIRST|NUMBER_FIRST_SUFFIX|NUMBER_LAST_PREFIX|NUMBER_LAST|NUMBER_LAST_SUFFIX|STREET_LOCALITY_PID|LEGAL_PARCEL_ID|LEVEL_GEOCODED_CODE|LONGITUDE|LATITUDE")
      for _, v := range olya.Data {

          fmt.Fprintln(f, v)
          if err != nil {
              fmt.Println(err)
              return
          }
      }
      err = f.Close()
      if err != nil {
          fmt.Println(err)
          return
      }
    }
  }
}

func main() {
    data, err := ioutil.ReadFile("lotyouraddress_streets_geo.csv")
    if err != nil {
        fmt.Println("File reading error", err)
        return
    }
    //fmt.Println("Contents of file:", string(data))
    for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
      //fmt.Println(line)
      LstLya = append(LstLya, line)
    }

    var wg sync.WaitGroup
    for _, r := range LstLya {
      wg.Add(1)
      go ProcessLineLOCALITY_PID(r,&wg)
      //fmt.Println(r_split[3])
    }

    wg.Wait()
    fmt.Println("LOCALITY_PID: ", len(LstLOCALITY_PID))

    var sslpwg sync.WaitGroup
    for _, r := range LstLya {
      sslpwg.Add(1)
      go SortSTREET_LOCALITY_PID(r,&sslpwg)
    }

    sslpwg.Wait()
    //tas,act,nt,sa
    SortState("SA_ADDRESS_DETAIL_Extracted.txt")
    WriteSuburbID()
}
