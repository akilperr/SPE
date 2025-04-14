defmodule SpeTest do
  use ExUnit.Case
  doctest Spe

  test "greets the world" do
    assert Spe.hello() == :world
  end
end
