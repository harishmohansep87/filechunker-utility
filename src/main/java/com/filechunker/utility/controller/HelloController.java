package com.filechunker.utility.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {
	
	@GetMapping("/api/javainuse")
	public String sayHello() {
		return "Swagger Hello World";
	}
}
