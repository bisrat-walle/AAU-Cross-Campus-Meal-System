import { Injectable } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { SharedService } from './shared.service';

@Injectable({
  providedIn: 'root'
})
export class AdminGuardGuard implements CanActivate {
  constructor(private service:SharedService, private router:Router){}
  role:any;
  canActivate():boolean{
    
    this.service.getUserRole().subscribe(
	(data:any) =>{
		this.role = data['role'];
	});
	if ( this.role == "ADMIN"){
		return true;
	}
	this.router.navigate(['/scan']);
	return false;
  }
  
}