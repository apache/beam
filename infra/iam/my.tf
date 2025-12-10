data "external" "run" {
  program = ["bash", "-c", "$(chmod +x myscript.sh; ./myscript.sh);{}"]
}