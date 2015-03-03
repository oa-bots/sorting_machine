# -*- coding: utf-8 -*-
 
require 'json'
require 'turbotlib'
require 'active_support/all'
 
Turbotlib.log("Starting run...")

class IronMQ

  def initialize(token, project_id, queue, host)
    @token = token
    @project_id = project_id
    @queue = queue
    @host = host
  end
  
  def project_url
    "http://#{@host}/1/projects/#{@project_id}"
  end
  
  def get
    response = HTTPClient.new.get "#{project_url}/queues/#{@queue}/messages", nil, [["Authorization", "OAuth #{@token}"]]
    JSON.parse(response.content)["messages"].first
  end
  
  def delete(message)
    response = HTTPClient.new.delete "#{project_url}/queues/#{@queue}/messages/#{message["id"]}", nil, [["Authorization", "OAuth #{@token}"]]
  end

end

queue = IronMQ.new(ENV['TOKEN'], ENV['ID'], ENV['QUEUE'], 'mq-aws-eu-west-1.iron.io')

msg = queue.get
while !msg.nil? do
  # We just need to dump to stdout, no transformation required. Don't even need to parse it.
  puts msg["body"]
  # Clear it from the queue unless in draft
  queue.delete msg unless ENV['RUN_TYPE'] == "draft"
  # and get the next one
  msg = queue.get
end