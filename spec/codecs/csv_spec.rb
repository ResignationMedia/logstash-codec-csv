# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/csv"
require "logstash/event"

describe LogStash::Codecs::CSV do

  subject(:codec) { LogStash::Codecs::CSV.new(config) }
  let(:config)    { Hash.new }

  before(:each) do
    codec.register
  end

  describe "encode" do
    let(:csv_data) { 
      {
        "column1" => "big",
        "column2" => "bird",
        "column3" => "sesame street",
        "column4" => "extra data",
      }
    }
    let(:csv_string) { "big,bird,sesame street\n" }
    let(:columns) { ["column1", "column2", "column3"] }

    context "with columns" do
      let(:config) { {"columns" => columns} }
      let(:event) { LogStash::Event.new(csv_data) }
      let(:csv_parse_options) do
        {
          :headers => columns, 
          :return_headers => false
        }
      end

      it "should return CSV encoded string" do
        got_event = false
        codec.on_event do |event, data|
          expect(data).to eq(csv_string)
          expect(CSV.parse(data, csv_parse_options)["column1"][0]).to eq("big") 
          expect(CSV.parse(data, csv_parse_options)["column2"][0]).to eq("bird") 
          expect(CSV.parse(data, csv_parse_options)["column3"][0]).to eq("sesame street") 
          got_event = true
        end
        codec.encode(event)
        expect(got_event).to eq(true)
      end

      it "should return CSV encoded string in column order" do
        got_event = false
        codec.on_event do |event, data|
          expect(CSV.parse(data, csv_parse_options).headers()).to include("column1").and include("column2").and include("column3")
          expect(CSV.parse(data, csv_parse_options).headers()).to eq(columns)
          expect(CSV.parse(data, csv_parse_options).headers()).not_to include("column4")
          got_event = true
        end
        codec.encode(event)
        expect(got_event).to eq(true)
      end
    end

    context "without columns" do
      let(:config) { Hash.new }

      it "should return CSV encoded string" do
        event = LogStash::Event.new(csv_data)
        got_event = false
        codec.on_event do |event, data|
          expect(CSV.parse(data)).to be_instance_of(Array) 
          expect(CSV.parse(data)[0]).to include("big").and include("bird").and include("sesame street").and include("extra data")
          got_event = true
        end
        codec.encode(event)
        expect(got_event).to eq(true)
      end
    end
  end

  describe "decode" do

    let(:data) { "big,bird,sesame street" }

    it "return an event from CSV data" do
      codec.decode(data) do |event|
        expect(event.get("column1")).to eq("big")
        expect(event.get("column2")).to eq("bird")
        expect(event.get("column3")).to eq("sesame street")
      end
    end

    describe "given column names" do
      let(:doc)    { "big,bird,sesame street" }
      let(:config) do
        { "columns" => ["first", "last", "address" ] }
      end

      it "extract all the values" do
        codec.decode(data) do |event|
          expect(event.get("first")).to eq("big")
          expect(event.get("last")).to eq("bird")
          expect(event.get("address")).to eq("sesame street")
        end
      end

      context "parse csv skipping empty columns" do

        let(:data)    { "val1,,val3" }

        let(:config) do
          { "skip_empty_columns" => true,
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        it "extract all the values" do
          codec.decode(data) do |event|
            expect(event.get("custom1")).to eq("val1")
            expect(event.to_hash).not_to include("custom2")
            expect(event.get("custom3")).to eq("val3")
          end
        end
      end

      context "parse csv without autogeneration of names" do

        let(:data)    { "val1,val2,val3" }
        let(:config) do
          {  "autogenerate_column_names" => false,
             "columns" => ["custom1", "custom2"] }
        end

        it "extract all the values" do
          codec.decode(data) do |event|
            expect(event.get("custom1")).to eq("val1")
            expect(event.get("custom2")).to eq("val2")
            expect(event.get("column3")).to be_falsey
          end
        end
      end

    end

    describe "custom separator" do
      let(:data) { "big,bird;sesame street" }

      let(:config) do
        { "separator" => ";" }
      end

      it "return an event from CSV data" do
        codec.decode(data) do |event|
          expect(event.get("column1")).to eq("big,bird")
          expect(event.get("column2")).to eq("sesame street")
        end
      end
    end

    describe "quote char" do
      let(:data) { "big,bird,'sesame street'" }

      let(:config) do
        { "quote_char" => "'"}
      end

      it "return an event from CSV data" do
        codec.decode(data) do |event|
          expect(event.get("column1")).to eq("big")
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq("sesame street")
        end
      end

      context "using the default one" do
        let(:data) { 'big,bird,"sesame, street"' }
        let(:config) { Hash.new }

        it "return an event from CSV data" do
          codec.decode(data) do |event|
            expect(event.get("column1")).to eq("big")
            expect(event.get("column2")).to eq("bird")
            expect(event.get("column3")).to eq("sesame, street")
          end
        end
      end

      context "using a null" do
        let(:data) { 'big,bird,"sesame" street' }
        let(:config) do
          { "quote_char" => "\x00" }
        end

        it "return an event from CSV data" do
          codec.decode(data) do |event|
            expect(event.get("column1")).to eq("big")
            expect(event.get("column2")).to eq("bird")
            expect(event.get("column3")).to eq('"sesame" street')
          end
        end
      end
    end

    describe "having headers" do

      let(:data) do
        [ "size,animal,movie", "big,bird,sesame street"]
      end

      let(:new_data) do
        [ "host,country,city", "example.com,germany,berlin"]
      end

      let(:config) do
        { "include_headers" => true }
      end

      it "include header information when requested" do
        codec.decode(data[0]) # Read the headers
        codec.decode(data[1]) do |event|
          expect(event.get("size")).to eq("big")
          expect(event.get("animal")).to eq("bird")
          expect(event.get("movie")).to eq("sesame street")
        end
      end

      it "reset headers and fetch the new ones" do
        data.each do |row|
          codec.decode(row)
        end
        codec.reset
        codec.decode(new_data[0]) # set the new headers
        codec.decode(new_data[1]) do |event|
          expect(event.get("host")).to eq("example.com")
          expect(event.get("country")).to eq("germany")
          expect(event.get("city")).to eq("berlin")
        end
      end
    end

    describe "using field convertion" do

      let(:config) do
        { "convert" => { "column1" => "integer", "column3" => "boolean" } }
      end
      let(:data)   { "1234,bird,false" }

      it "get converted values to the expected type" do
        codec.decode(data) do |event|        
          expect(event.get("column1")).to eq(1234)
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq(false)
        end
      end

      context "when using column names" do

        let(:config) do
          { "convert" => { "custom1" => "integer", "custom3" => "boolean" },
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        it "get converted values to the expected type" do
          codec.decode(data) do |event|
            expect(event.get("custom1")).to eq(1234)
            expect(event.get("custom2")).to eq("bird")
            expect(event.get("custom3")).to eq(false)
          end
        end
      end
    end

  end
end
