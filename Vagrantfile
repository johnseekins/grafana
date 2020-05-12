# -*- mode: ruby -*-
# vi: set ft=ruby :
 
Vagrant.configure("2") do |config|
  config.vm.box = "generic/ubuntu1804"
  # config.vm.box_check_update = false
  config.vm.synced_folder ".", "/vagrant_data"
  config.vm.provider :libvirt do |libvirt|
    libvirt.memory = "4096"
    libvirt.cpus = 4
  end
 
  config.vm.provision "shell", inline: <<-SHELL
     curl -sL https://deb.nodesource.com/setup_12.x | bash -
     curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
     echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
     add-apt-repository -y ppa:longsleep/golang-backports
     apt-get update
     apt-get install -y build-essential golang-go yarn nodejs ruby ruby-dev rubygems
     gem install --no-document fpm
  SHELL
end
